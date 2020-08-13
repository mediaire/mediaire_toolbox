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
