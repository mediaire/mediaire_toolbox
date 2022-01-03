"""Metadata that indicates the version of the schema in the transactions DB.

We use our own schema migration routines. Don't forget to change the version
number here if you add more fields to the Transaction object, and implement
the migration SQL queries in migrations.py
"""

TRANSACTIONS_DB_SCHEMA_NAME = "TRANSACTION"
TRANSACTIONS_DB_SCHEMA_VERSION = 19

RETRY_DATABASE_OP_SECONDS = 5
RETRY_DATABASE_OP_TIMES = 360
