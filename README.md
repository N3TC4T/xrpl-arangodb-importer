# xrpl-arangodb-importer

## Install Requirements

```
python setup.py install
```

## Run

```
usage: main.py [-h] [-ah HOST] [-u USERNAME] [-p PASSWORD] [-s SOURCE] [-l LEDGER] [-c] [-d]

optional arguments:
  -h, --help            show this help message and exit
  -ah HOST, --host HOST
                        Arangodb url
  -u USERNAME, --username USERNAME
                        Arangodb username
  -p PASSWORD, --password PASSWORD
                        Arangodb password
  -s SOURCE, --source SOURCE
                        Node url or database path
  -l LEDGER, --ledger LEDGER
                        Ledger index start from
  -c, --clean           Clean database before import
  -d, --debug           Debug mode
 
./main.py -s /var/lib/rippled/db/ -h http://127.0.0.1:829 -u arangodb_username -p arangodb_pass -d -v
```
