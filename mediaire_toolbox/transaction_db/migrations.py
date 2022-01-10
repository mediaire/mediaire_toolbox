from mediaire_toolbox.transaction_db import index
import logging

default_logger = logging.getLogger(__name__)

"""SQL Commands that need to be issued in order to migrate the TransactionsDB
from one version to another. Keyed by target version ID."""
MIGRATIONS = {
    2: [
        "ALTER TABLE transactions ADD COLUMN task_progress INT DEFAULT 0",
        "UPDATE transactions SET task_progress = 10 WHERE processing_state = 'spm_lesion'",
        "UPDATE transactions SET task_progress = 10 WHERE processing_state = 'spm_volumetry'",
        "UPDATE transactions SET task_progress = 80 WHERE processing_state = 'volumetry_assessment'",
        "UPDATE transactions SET task_progress = 90 WHERE processing_state = 'report'",
        "UPDATE transactions SET task_progress = 100 WHERE processing_state = 'send_to_pacs'"
    ],
    3: [
        "ALTER TABLE transactions ADD COLUMN task_skipped INT DEFAULT 0",
    ],
    4: [
        "ALTER TABLE transactions ADD COLUMN task_cancelled INT DEFAULT 0",
    ],
    5: [
        "ALTER TABLE transactions ADD COLUMN status TEXT",
        "ALTER TABLE transactions ADD COLUMN institution TEXT",
        "ALTER TABLE transactions ADD COLUMN sequences TEXT",
        "UPDATE transactions SET status = 'sent_to_pacs' WHERE processing_state = 'send_to_pacs'",
        "UPDATE transactions SET status = 'unseen' WHERE processing_state != 'send_to_pacs'"
    ],
    6: [
        "ALTER TABLE transactions ADD COLUMN archived INT DEFAULT 0",
    ],
    7: [
        "ALTER TABLE transactions ADD COLUMN study_date TEXT",
    ],
    8: [
        "ALTER TABLE transactions ADD COLUMN patient_consent INT DEFAULT 0",
    ],
    9: [],
    10: [
        "ALTER TABLE transactions ADD COLUMN product_id INT DEFAULT 1"
    ],
    11: [
        "ALTER TABLE transactions ADD COLUMN data_uploaded DATETIME"
    ],
    12: [
        "ALTER TABLE transactions ADD COLUMN creation_date DATETIME"
    ],
    13: [
        "ALTER TABLE transactions ADD COLUMN billable TEXT"
    ],
    14: [
        "ALTER TABLE transactions ADD COLUMN version VARCHAR(31)",
        "ALTER TABLE transactions ADD COLUMN analysis_type VARCHAR(31)",
        "ALTER TABLE transactions ADD COLUMN qa_score VARCHAR(31)",
    ],
    15: [
        "CREATE INDEX index_p_a_s_t ON transactions(patient_id,analysis_type,study_date,transaction_id)"
    ],
    16: [
        "ALTER TABLE transactions ADD COLUMN priority INT DEFAULT 0"
    ],
    17: [
        "CREATE TABLE IF NOT EXISTS sites (id INT PRIMARY KEY, name TEXT);",
        "INSERT OR IGNORE INTO sites (id, name) VALUES (0, 'default');",
        # FOREIGN KEY column would need to be created via temporary table due
        # to sqlite's limited ALTER TABLE support: www.sqlite.org/faq.html#q11
        # so instead of doing a massive migration, we just ignore the formal
        # relationship between the tables for now.
        # "CREATE TEMPORARY TABLE transactions_backup (<full table def>);",
        # "INSERT INTO transactions_backup SELECT * FROM transactions;",
        # "DROP TABLE transactions;",
        # ("CREATE TABLE transactions ("
        #  "  <full table def>"
        #  "  FOREIGN KEY(site_id) REFERENCES sites(id)"
        #  "  CONSTRAINT taskstate CHECK ("
        #  "    task_state IN ('queued', 'processing', 'failed', 'completed')"
        #  "  )"
        #  ");"),
        # ("CREATE INDEX index_p_a_s_t ON"
        #  " transactions(patient_id,analysis_type,study_date,transaction_id);"),
        "ALTER TABLE transactions ADD COLUMN site_id INT;",
        "UPDATE transactions SET site_id = 0 WHERE site_id IS NULL;",
    ],
    18: [
        "CREATE TABLE IF NOT EXISTS users_sites ("
        "  user_id INTEGER NOT NULL,"
        "  site_id INTEGER NOT NULL,"
        "  PRIMARY KEY (user_id, site_id),"
        "  FOREIGN KEY (user_id) REFERENCES users(id),"
        "  FOREIGN KEY (site_id) REFERENCES sites(id)"
        ");"
    ],
    19: [
        "ALTER TABLE transactions ADD COLUMN patient_consent_date DATETIME DEFAULT NULL;",
        ("UPDATE transactions SET"
         "  patient_consent_date = creation_date "
         "WHERE"
         "  patient_consent = 1"
         "  AND data_uploaded IS NOT NULL"
         ";"),
        ("UPDATE transactions SET"
         "  patient_consent_date = datetime(0, 'unixepoch', 'localtime') "
         "WHERE"
         "  patient_consent = 1"
         "  AND data_uploaded IS NULL"
         ";"),
    ]
}


def migrate_institution(session, model):
    for transaction in session.query(model).all():
        index.set_index_institution(transaction)


def migrate_sequences(session, model):
    for transaction in session.query(model).all():
        index.set_index_sequences(transaction)


def migrate_study_date(session, model):
    for transaction in session.query(model).all():
        index.set_index_study_date(transaction)


def migrate_version(session, model):
    default_logger.warn("Indexing version")
    for transaction in session.query(model).all():
        try:
            index.set_index_version(transaction)
            session.add(transaction)
            session.commit()
        except Exception:
            default_logger.warn(
                "Failed to index version for transaction id: {}"
                .format(transaction.transaction_id))


def migrate_analysis_types(session, model):
    default_logger.warn("Indexing analysis_types")
    for transaction in session.query(model).all():
        try:
            index.set_index_analysis_type(transaction)
            session.add(transaction)
            session.commit()
        except Exception:
            default_logger.warn(
                "Failed to index analysis_type for transaction id: {}"
                .format(transaction.transaction_id))


def migrate_qa_scores(session, model):
    default_logger.warn("Indexing qa_scores")
    for transaction in session.query(model).all():
        try:
            index.set_index_report_qa(transaction)
            session.add(transaction)
            session.commit()
        except Exception:
            default_logger.warn(
                "Failed to index qa_score for transaction id: {}"
                .format(transaction.transaction_id))


MIGRATIONS_SCRIPTS = {
    5: [
        migrate_institution,
        migrate_sequences,
    ],
    7: [
        migrate_study_date
    ],
    14: [
        migrate_version,
        migrate_analysis_types,
        migrate_qa_scores
    ]
}
