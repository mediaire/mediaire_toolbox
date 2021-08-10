# MEDIAIRE_TOOLBOX

Shared toolbox for our pipelines. 
* Logging conventions.
* Queue / Daemon classes.
* TransactionsDB (a generic way of persisting state through our pipelines).
* Data cleaner (a tool to periodically clean data on folders).

[![Build Status](https://travis-ci.org/mediaire/mediaire_toolbox.svg?branch=master)](https://travis-ci.org/mediaire/mediaire_toolbox)


## DataCleaner

`whitelist`, `blacklist` and `priority_list` are all glob patterns. If in the
`priority_list` is `*.dcm` or `*dcm` pattern, then when deciding to remove dcm
files, all files are removed from that folder for consistency.


## Migrations

Add an entry in migrate.py, and then change the version number in constants.py


## Running programmatic migrations manually

E.g. we want to run programmatic migration number 5 manually:

```
from mediaire_toolbox.transaction_db.transaction_db import migrate_scripts
from mediaire_toolbox.transaction_db.transaction_db import TransactionDB
from sqlalchemy import create_engine

engine = create_engine('sqlite:///t.db')
t_db = TransactionDB(engine)
migrate_scripts(t_db.session, engine, 4, 5)
```


## Running tests locally

To run the tests locally without Docker, create a virtual environment
```
pyenv install 3.5.10
pyenv local 3.5.10
pipenv --python 3.5.10
pipenv install
```
And then run
```
pipenv run nosetests tests/
```


## Test Resource Manager with real redis

The resource manager tests use `redislite` and automatically start up a full
internal redis server.

To monitor the redis server while the tests for the resource manager are
running, use
```
make test_resource_manager_local
```
to run the test. Then you can attach the redis montir via
```
make redis_monitor
```
To interact with the redis database directly, use
```
make redis_cli
```
