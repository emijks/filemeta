#! /usr/bin/env python3

import re
import os
import socket
import yaml
import json
import sys
import threading
import argparse
import paramiko
import pandas as pd


def parse_date(path, pattern=r'(\d{4}-\d{2}-\d{2})'):
    match = re.search(pattern, path)
    if not match:
        return pd.NaT
    return pd.to_datetime(match.group(1), errors='coerce')

def parse_sample_name(path):
    basename = path.split('/')[-1]
    if basename.startswith('GEX') | basename.startswith('MUX'):
        return '_'.join(basename.split('_')[:3])
    elif basename.startswith('ic'):
        return '_'.join(basename.split('_')[1:4])
    else:
        return basename.split('_')[0]

def parse_sample_id(path):
    basename = path.split('/')[-1]
    s_name = parse_sample_name(basename)
    start = basename.find(s_name)
    for i in ['_L00', '_R', '.fastq']:
        if i in basename:
            end = basename.find(i) 
            break
    else:
        end = len(basename)
    basename = basename[start:end]
    return basename

def parse_sample_type(path):
    basename = path.split('/')[-1]
    major_folder = path.split('/')[-2]
    if 'amplicons' in major_folder:
        return 'amplicon' 
    elif ('alpha' in basename) | ('beta' in basename):
        return 'bulkTCR'
    elif ('nanopore' in major_folder) | ('2025-09-23_fq' == major_folder):
        return 'NanoporeWGS'
    elif ('IAR_TCR' in major_folder) | ('scTCR' in major_folder):
        return 'scRNA+VDJ'
    elif ('scrna' in major_folder) | ('scRNA' in major_folder) | ('sc' in major_folder):
        return 'scRNA'
    elif ('atac' in major_folder):
        return 'ATAC'
    else:
        return 'WGS'


class FileParser:
    def list_files(self, paths: list[str], exts: list[str] | None = None) -> list[str]:
        raise NotImplementedError


class LocalParser(FileParser):
    def list_files(self, paths, exts):
        fpaths = []
        for path in paths:
            for root, _, files in os.walk(path):
                for f in files:
                    if not exts or any(f.endswith(ext) for ext in exts):
                        fpaths.append(os.path.join(root, f))
        return fpaths


class SSHParser(FileParser):
    def __init__(self, ssh):
        self.ssh = ssh

    def read_stream(self, stream, callback):
        for line in iter(stream.readline, ""):
            callback(line.strip())

    def list_files(self, paths, exts):
        fpaths = []
        findnames = '\\( ' + " -o ".join([f"-name '*{ext}'" for ext in exts]) + ' \\)' if exts else ''
        for path in paths:
            cmd = f"find {path} -type f " + findnames
            _, stdout, stderr = self.ssh.exec_command(cmd)
            err_thread = threading.Thread(target=self.read_stream, args=(stderr, print))
            err_thread.start()
            self.read_stream(stdout, fpaths.append)
        return fpaths


def get_file_parser(host_id: str, host_meta: dict):
    if host_id == 'local' or socket.gethostname() in host_meta.get('aliases', []):
        return LocalParser()
    ssh_client = get_ssh_client(host_meta['host'], host_meta['user'], host_meta.get('port', 22), host_meta.get('key', None))
    return SSHParser(ssh_client)

def get_ssh_client(
    host: str,
    user: str,
    port: int | str | None = None,
    key: str | None = None
) -> paramiko.SSHClient:
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(
        hostname=host,
        port=port or 22,
        username=user,
        key_filename=os.path.expanduser(key) if key else key,
    )
    return ssh_client

def read_config(config: str) -> dict:
    dir_path = os.path.dirname(__file__)
    config_path = os.path.join(dir_path, config)
    with open(config_path, 'r') as fh: 
        return yaml.safe_load(fh)

def parse_fpaths(paths: list[str] | None = None, exts: list[str] | None = None) -> list[str]:
    config = {'local': {'paths': paths, 'exts': exts}} if paths else read_config('config.yaml')
    fpaths = []
    try:
        for host_id, paths, exts, file_parser in [
            (host_id, host_meta['paths'], host_meta.get('exts'), get_file_parser(host_id, host_meta)) for host_id, host_meta in config.items()
        ]:
            print(f'Parsing fpaths from "{host_id}" ...')
            fpaths += file_parser.list_files(paths, exts)
    except Exception as e:
        print(repr(e))
        sys.exit(1)
    return fpaths

def aggregate_fpaths(fpaths: list[str]) -> pd.DataFrame:
    print(f'Aggregating fpaths ({len(fpaths)}) ...')
    filemeta = pd.DataFrame(columns=['sample_name', 'sample_id', 'sample_type', 'recieved', 'fpath'])
    filemeta['fpath'] = fpaths
    if not filemeta.empty:
        filemeta[['sample_name', 'sample_id', 'sample_type', 'recieved']] = filemeta.apply(lambda row: [
            parse_sample_name(row['fpath']),
            parse_sample_id(row['fpath']),
            parse_sample_type(row['fpath']),
            parse_date(row['fpath']),
        ], axis=1, result_type='expand')
    return filemeta

def parse_filemeta(paths: list[str] | None = None, exts: list[str] | None = None) -> pd.DataFrame:
    return aggregate_fpaths(parse_fpaths(paths, exts))


if __name__ == '__main__':
    arparser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    arparser.add_argument('-p', '--paths', nargs='+', help='Local paths to parse. Config ignored then.')
    arparser.add_argument('-e', '--exts', nargs='+', help='Specified file extensions to parse.')
    arparser.add_argument('-o', '--output', default='filemeta.csv', help='Output CSV path')
    args = arparser.parse_args()
    filemeta = parse_filemeta(args.paths, args.exts)
    if len(filemeta) == 0:
        print('No files found to export')
        sys.exit(0)
    filemeta.to_csv(args.output, sep='\t', index=False)
    print(f'Exported to "{args.output}".')