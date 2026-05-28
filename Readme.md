# FileMeta

## Requirements
`pip install requirements.txt`


## Configuration

### Hosts (`hosts`)

Defines SSH-accessible hosts.

- `host`: Hostname or IP address.  
- `user`: Username for SSH access.  
- `port`: Optional SSH port (default: 22).  
- `pkey`: Optional path to a private SSH key file for authentication.  
- `proxy`: Optional SSH host to use as a jump/proxy server. The value must match one of the hosts defined in the `hosts` section of the configuration.  
- `aliases`: Optional alternative names for the host. Can be used to identify the host as local or remote.  


### Database (`database`)

Specifies database connection settings.

- `creds`: Path to a JSON file containing database credentials.  
  The JSON file should have the following structure:

  ```json
  {
      "host": "hostname_or_ip",
      "port": 5432,
      "name": "database_name",
      "user": "username",
      "pass": "password"
  }
  ```

- `table`: Name of the table where file metadata will be stored.  
- `proxy`: Optional SSH host to use as a jump/proxy server. The value must match one of the hosts defined in the `hosts` section of the configuration.  


### Paths (`paths`)

Defines directories to scan for files on each host.  
The `host` can also be set to `local` to scan directories on the machine where the parser runs.

- Each entry has:  
  - `path`: Absolute path to the directory.  
  - `exts`: List of file extensions to include.  
  - `tag`: Label to assign to all files in this directory.  


### File Types (`ftypes`)

Defines common file types mapped to extensions.

Example:  
```
ftypes:
  fastq: ['.fq.gz', '.fastq.gz']
  vcf: ['.vcf.gz', '.vcf']
```


## Usage

1. Update your configuration with hosts, paths, and database info.
2. Prepare your database credentials JSON file.
3. Run the file parser:

```
filemeta.py [-h] [-p PATHS [PATHS ...]] [-e EXTS [EXTS ...]] [-m MD5] [-o OUTPUT] [-E]

options:
  -h, --help            show this help message and exit
  -p PATHS [PATHS ...], --paths PATHS [PATHS ...]
                        Local paths to parse. Hosts and paths config ignored then. (default: None)
  -e EXTS [EXTS ...], --exts EXTS [EXTS ...]
                        Specified file extensions to parse. (default: None)
  -m MD5, --md5 MD5     Checksum md5 TSV fpath (default: None)
  -o OUTPUT, --output OUTPUT
                        Output TSV path (default: None)
  -E, --export          Export to PostgreSQL (default: False)
```

The MD5 checksum TSV file is expected to contain two columns without a header: checksum and filepath.