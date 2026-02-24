#! /usr/bin/env python3

import re
import os
import socket
import yaml
import json
import sys
import threading
import argparse
import pandas as pd
import paramiko
from contextlib import contextmanager, nullcontext
from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine


class FileParser:
    def list_files(self, path: str, exts: list[str] | None = None) -> list[str]:
        raise NotImplementedError

class LocalParser(FileParser):
    def list_files(self, path: str, exts: list[str] | None = None) -> list[str]:
        fpaths = []
        for root, _, files in os.walk(path):
            for f in files:
                if not exts or any(f.endswith(ext) for ext in exts):
                    fpaths.append(os.path.join(root, f))
        return fpaths

class SSHParser(FileParser):
    def __init__(self, ssh_client: paramiko.SSHClient):
        self.ssh_client = ssh_client

    def read_stream(self, stream, callback):
        for line in iter(stream.readline, ""):
            callback(line.strip())

    def list_files(self, path: str, exts: list[str] | None = None) -> list[str]:
        fpaths = []
        findnames = '\\( ' + " -o ".join([f"-name '*{ext}'" for ext in exts]) + ' \\)' if exts else ''
        cmd = f"find {path} -type f " + findnames
        _, stdout, stderr = self.ssh_client.exec_command(cmd)
        err_thread = threading.Thread(target=self.read_stream, args=(stderr, print))
        err_thread.start()
        self.read_stream(stdout, fpaths.append)
        return fpaths


class MetaParser:
    """
    Parse and extract metadata from file path.
    """

    @classmethod
    def parse_basename(cls, path):
        return path.split('/')[-1]

    @classmethod
    def parse_ftype(cls, path, ftype_map={}):
        for ext, ftype in ftype_map.items():
            if path.endswith(ext):
                return ftype
        return None

    @classmethod
    def parse_date(cls, path, pattern=r'(\d{4}-\d{2}-\d{2})'):
        match = re.search(pattern, path)
        if not match:
            return pd.NaT
        return pd.to_datetime(match.group(1), errors='coerce')

    @classmethod
    def parse_sample(cls, path):
        basename = cls.parse_basename(path)
        if basename.startswith('GEX') | basename.startswith('MUX'):
            return '_'.join(basename.split('_')[:3])
        elif basename.startswith('ic'):
            return '_'.join(basename.split('_')[1:4])
        else:
            return basename.split('_')[0].split('.')[0]

    @classmethod
    def parse_sample_id(cls, path):
        sample = cls.parse_sample(path)
        if not sample:
            return None
        if 8 <= len(sample) <= 12:
            return sample[:8]
        return None

    @classmethod
    def parse_sample_type(cls, path, default_sample_type=None):
        basename = cls.parse_basename(path)
        major_folder = path.split('/')[-2]
        if 'amplicons' in path:
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
            return default_sample_type


class HostManager:
    """
    Hosts SSH client and tunnel manager
    """
    def __init__(self, hosts_conf: dict):
        self.hosts_conf = hosts_conf
        self._chains = self._resolve_chains()

    def _resolve_chains(self) -> dict:
        chains = {}
        for host_name in self.hosts_conf:
            chains[host_name] = self._resolve_chain(host_name)
        return chains

    def _resolve_chain(self, host_name: str) -> list:
        chain = []
        visited = set()
        current = host_name
        while current:
            if current in visited:
                raise ValueError(f"Cyclic proxy detected at host '{current}'")
            visited.add(current)
            host_conf = self.hosts_conf.get(current)
            if host_conf is None:
                raise ValueError(f"Host '{current}' not found in config")
            chain.append(host_conf)
            current = host_conf.get('proxy')
        return list(reversed(chain))

    def _build_ssh_chain(self, chain: list) -> tuple:
        ssh_clients = []
        transport = None
        for host in chain:
            print(f'SSH connection to {host["host"]} ...')
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            pkey = os.path.expanduser(host['pkey']) if host.get('pkey') else None
            if transport:
                sock = transport.open_channel(
                    'direct-tcpip',
                    (host['host'], host.get('port', 22)),
                    ('127.0.0.1', 0)
                )
                ssh.connect(
                    host['host'],
                    port=host.get('port', 22),
                    username=host['user'],
                    sock=sock,
                    pkey=pkey
                )
            else:
                ssh.connect(
                    host['host'],
                    port=host.get('port', 22),
                    username=host['user'],
                    pkey=pkey
                )
            transport = ssh.get_transport()
            ssh_clients.append(ssh)
        return ssh_clients, transport

    def ssh_client(self, host_name: str) -> paramiko.SSHClient:
        if host_name not in self._chains:
            raise ValueError(f"Unknown host '{host_name}'")
        chain = self._chains[host_name]
        ssh_clients, _ = self._build_ssh_chain(chain)
        last_ssh = ssh_clients[-1]
        return last_ssh

    def ssh_tunnel(self, host_name: str, remote_bind_address: tuple) -> SSHTunnelForwarder:
        if host_name not in self._chains:
            raise ValueError(f"Unknown host '{host_name}'")
        chain = self._chains[host_name]
        ssh_clients, _ = self._build_ssh_chain(chain)
        last_ssh = chain[-1]
        print(f'SSH forwading to {remote_bind_address[0]} ...')
        tunnel = SSHTunnelForwarder(
            ssh_address_or_host=last_ssh['host'],
            ssh_username=last_ssh['user'],
            ssh_pkey=last_ssh.get('pkey'),
            remote_bind_address=remote_bind_address,
            local_bind_address=('127.0.0.1', 0)
        )
        return tunnel

    def is_local(self, host_name: str) -> bool:
        if host_name == 'local' or socket.gethostname() in self.hosts_conf[host_name].get('aliases', []):
            return True
        return False


class DatabaseConnector:
    """
    PostgreSQL database connection utility.
    """
    def __init__(self, db_creds: dict, ssh_tunnel: SSHTunnelForwarder | None = None):
        self.db_creds = db_creds
        self.ssh_tunnel = ssh_tunnel

    @contextmanager
    def conn(self):
        with self.ssh_tunnel or nullcontext() as tunnel:
            host = '127.0.0.1' if tunnel else self.db_creds["host"]
            port = tunnel.local_bind_port if tunnel else self.db_creds["port"]
            engine = create_engine((
                f"postgresql+psycopg2://"
                f"{self.db_creds['user']}:{self.db_creds['pass']}"
                f"@{host}:{port}/{self.db_creds['name']}"
            ))
            with engine.connect() as connection:
                try:
                    yield connection
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise

@contextmanager
def database_connection(database_config: dict, hostmanager: HostManager):
    db_creds = read_json(database_config['creds'])
    if database_config.get('proxy'):
        with hostmanager.ssh_tunnel(
            host_name=database_config['proxy'],
            remote_bind_address=(db_creds['host'], db_creds['port'])
        ) as tunnel:
            with DatabaseConnector(db_creds, tunnel).conn() as conn:
                yield conn
    else:
        with DatabaseConnector(db_creds, None).conn() as conn:
            yield conn


def parse_filemeta(paths_config: dict, hostmanager: HostManager, ftype_map: dict = {}) -> pd.DataFrame:
    filemeta = pd.DataFrame(columns=['sample', 'sample_id', 'fname', 'ftype', 'sample_type', 'host', 'tag', 'recieved', 'fpath'])
    try:
        for host_name, paths_meta in paths_config.items():
            print(f'Parsing host "{host_name}" ...')
            with file_parser(host_name, hostmanager) as parser:
                for path_meta in paths_meta:
                    print(f'Parsing and aggregating fpaths from "{path_meta["path"]}" ...')
                    fpaths = parser.list_files(path_meta['path'], path_meta['exts'])
                    add = aggregate_fpaths(fpaths=fpaths, host=host_name, tag=path_meta.get('tag'), ftype_map=ftype_map, default_sample_type='WGS' if path_meta.get('tag')=='CSP' else None)
                    filemeta = pd.concat([filemeta, add])
    except Exception as e:
        print(repr(e))
        sys.exit(1)
    return filemeta

def aggregate_fpaths(fpaths: list[str], host: str | None = None, tag: str | None = None, ftype_map: dict = {}, default_sample_type: str | None = None) -> pd.DataFrame:
    filemeta = pd.DataFrame(columns=['sample', 'sample_id', 'fname', 'ftype', 'sample_type', 'host', 'tag', 'recieved', 'fpath'])
    filemeta['fpath'] = fpaths
    if not filemeta.empty:
        filemeta[['sample', 'sample_id', 'fname', 'ftype', 'sample_type', 'host', 'tag', 'recieved']] = filemeta.apply(lambda row: [
            MetaParser.parse_sample(row['fpath']),
            MetaParser.parse_sample_id(row['fpath']),
            MetaParser.parse_basename(row['fpath']),
            MetaParser.parse_ftype(row['fpath'], ftype_map),
            MetaParser.parse_sample_type(row['fpath'], default_sample_type),
            host,
            tag,
            MetaParser.parse_date(row['fpath']),
        ], axis=1, result_type='expand')
    return filemeta

@contextmanager
def file_parser(host_name: str, hostmanager: HostManager) -> FileParser:
    if hostmanager.is_local(host_name):
        yield LocalParser()
    else:
        with hostmanager.ssh_client(host_name) as ssh_client:
            yield SSHParser(ssh_client)


def setup_config(config_path: str, paths: list[str] | None = None, exts: list[str] | None = None) -> dict:
    config = read_yaml(config_path)
    config['hosts']['local'] = {}
    config['ftype_map'] = invert_mapping(config.get('ftypes', {}))
    if paths:
        config['paths'] = {'local': [
            {'path': path, 'exts': exts} for path in paths
        ]}
    return config


def read_yaml(yaml_path: str) -> dict:
    dir_path = os.path.dirname(__file__)
    yaml_path = os.path.join(dir_path, yaml_path)
    with open(yaml_path, 'r') as fh: 
        return yaml.safe_load(fh)

def read_json(json_path: str) -> dict:
    dir_path = os.path.dirname(__file__)
    json_path = os.path.join(dir_path, json_path)
    with open(json_path, 'r') as fh:
        return json.load(fh)

def invert_mapping(d: dict) -> dict:
    """
    Convert a {key: [values]} mapping into {value: key}.
    """
    return {value: key for key, values in d.items() for value in values}


if __name__ == '__main__':
    arparser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    arparser.add_argument('-p', '--paths', nargs='+', help='Local paths to parse. Hosts and paths config ignored then.')
    arparser.add_argument('-e', '--exts', nargs='+', help='Specified file extensions to parse.')
    arparser.add_argument('-m', '--md5', help='checksum md5 fpath')
    arparser.add_argument('-o', '--output', help='Output CSV path')
    arparser.add_argument('-E', '--export', action='store_true', help='Export to PostgreSQL')
    args = arparser.parse_args()

    config = setup_config('config.yaml', args.paths, args.exts)
    hostmanager = HostManager(config['hosts'])
    filemeta = parse_filemeta(config['paths'], hostmanager, config['ftype_map'])
    if len(filemeta) == 0:
        print('No files found to export')
        sys.exit(0)

    if args.md5:
        try:
            print(f'Updating md5 checksums by "{args.md5}" ...')
            md5 = pd.read_csv(args.md5, sep='  ', header=None, names=['md5', 'fpath'], engine='python')
            filemeta = filemeta.merge(md5, how='left', on='fpath')
        except Exception as e:
            print(f'Failed to parse and update checksums: {repr(e)}')

    if args.output:
        print(f'Exporting to file "{args.output}" ...')
        filemeta.to_csv(args.output, sep='\t', index=False)
        print(f'Exported to file "{args.output}".')

    if args.export:
        tablename = config['database']['table']
        print(f'Exporting to PostgreSQL table "{tablename}" ...')
        with database_connection(config['database'], hostmanager) as conn:
            filemeta.to_sql(tablename, con=conn, if_exists="append", index=False)
            print(f'Exported to PostgreSQL table "{tablename}".')
