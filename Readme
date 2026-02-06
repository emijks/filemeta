# FileMeta

# Requirements
`pip install requirements.txt`

# Config
`config.yaml` example
```
local:
  paths:
    - /home/user

cluster:
  aliases: ['calc']
  host: calc.cod.phystech.edu
  port: 22
  user: safin.ef
  key: ~/.ssh/id_ed25519
  paths:
    - /slowhome/common/LGI/CSP
  exts:
    - vcf.gz
    - fastq.gz
```
Required host meta:
- `paths`
- also `host`, `user`, `paths` for remote
Optional host meta: `aliases`, `port`, `key`, `exts`
If host-header is `local` or hostname is in `aliases` then local parsing applied.
Config is ignored if CLI/API `paths` provided (forced local parse).

# CLI
./filemeta.py [-h] [-p PATHS [PATHS ...]] [-e EXTS [EXTS ...]] [-o OUTPUT]

options:
  -h, --help            show this help message and exit
  -p, --paths PATHS [PATHS ...]
                        Local paths to parse. Config ignored then. (default: None)
  -e, --exts EXTS [EXTS ...]
                        Specified file extensions to parse. (default: None)
  -o, --output OUTPUT   Output CSV path (default: filemeta.csv)

# API
```
import filemeta
filemeta.parse_filemeta()
```