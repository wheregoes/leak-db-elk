# leak-db-v2.py
**OUTDATED - Description - NEED UPDATE** <br />
A simple leak database script. <br />

**Requirements:**
```pip install tqdm elasticsearch```

**Features** <br />
:heavy_check_mark: Use elasticsearch to store the results. <br />
:heavy_check_mark: The user can check if the leaks was already stored. <br />
:heavy_check_mark: Supports combolist format (user:pass or email:pass). <br />
:heavy_check_mark: Supports infostealer logs (needs to be parsed to csv) format (url,user,pass or url,email,pass). <br />
:heavy_check_mark: Can be used in background. <br />

**Future Updates** <br />
***Suggestions***

Usage:
```
usage: leak-db-v2.py [--combolist] [--infostealer] file_path

Leak Database

positional arguments:
  file_path      Path to the input file

options:
  -h, --help     Help
  --combolist    Process combolist file
  --infostealer  Process infostealer file
```
