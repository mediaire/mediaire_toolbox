from typing import Optional
import datetime

from sqlalchemy import (
    Column, Integer, String, Sequence, DateTime, Date, Enum, ForeignKey
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import validates
from passlib.apps import custom_app_context as pwd_context

from mediaire_toolbox.task_state import TaskState
from mediaire_toolbox import constants

Base = declarative_base()


def validate_utc(key, datetime_obj):
    """Only accepts `datetime_obj` in UTC or `None` for `key`."""
    if datetime_obj and datetime_obj.tzinfo != datetime.timezone.utc:
        raise ValueError("{} only accepts UTC values".format(key))
    return datetime_obj


# TODO Change to a dataclass when moving to Python 3.7
class Transaction(Base):

    __tablename__ = 'transactions'

    """Mediaire transaction table"""
    transaction_id = Column(Integer, Sequence(
        'transaction_id'), primary_key=True)

    # study instance uid dicom tag
    study_id = Column(String(255))
    # patient id dicom tag
    patient_id = Column(String(255))
    # patient name dicom tag
    name = Column(String(255))
    # patient birth date dicom tag
    birth_date = Column(Date())
    # TODO convert this to date
    # study date dicom tag, in string format 'YYYYMMDD'
    study_date = Column(String())
    # 1 if this transaction has patient consent
    patient_consent = Column(Integer, default=0)
    # institution dicom tag indexed from DICOM header, for free text search
    institution = Column(String())

    # Datetime when the transaction moved to 'processing' state for
    # the first time
    start_date = Column(DateTime())
    # Datetime when the transaction moved to 'completed' state for
    # the first time
    end_date = Column(DateTime())
    # Datetime when the transaction was created
    creation_date = Column(DateTime())

    # transaction types
    # mdbrain version
    version = Column(String(31))
    # analysis type: ['mdbrain_ms', 'mdbrain_nd', 'mdspine_ms']
    analysis_type = Column(String(31))
    # qa score of the transaction: ['rejected', 'good', 'acceptable']
    # If the value is 'rejected', the analysis will create a
    # bad qa score report
    qa_score = Column(String(31))
    # Product id of the transaction, 1 (mdbrain) or 2 (mdspine)
    product_id = Column(Integer, default=1)

    # transaction states
    # Current state of the transaction:
    # ['completed', 'failed', 'processing', 'queued']

    # Completed - analysis ran through without any error
    # Failed - An error occurred. For most of the known errors,
    #          see md_commons/exceptions
    # Processing - the analysis is currenlty being processed
    # Queued - the analysis is currenlty being held in the queue, because
    # another analysis is currenlty already being processed

    # Transition of states:   queued - > processing -> completed/failed

    task_state = Column(Enum(TaskState))
    # Component name of the current task:
    # ['brain_segmentation', 'lesion_segmentation', ....]
    processing_state = Column(String(255))
    # task progress of the transaction,
    # int between 0 (start) and 100 (finished).
    task_progress = Column(Integer, default=0)
    # 1 if this transaction was skipped
    task_skipped = Column(Integer, default=0)
    # 1 if this transaction was cancelled
    task_cancelled = Column(Integer, default=0)
    # 1 if this transaction was archived
    archived = Column(Integer, default=0)

    # Error message of the transaction, if it failed
    error = Column(String())
    # new platform status: unseen / reviewed / sent_to_pacs
    status = Column(String())
    # series description of selected sequences that are processed.
    # indexed from Task object, for free text search
    sequences = Column(String())
    # the json object of the Task object
    last_message = Column(String)

    # misc
    # DateTime when transaction data was exported to client api
    data_uploaded = Column(DateTime())
    # Transaction should be billed if empty
    billable = Column(String())
    # priority -> integer number that can affect the order in which
    # transactions are dequeued
    priority = Column(Integer, default=0)

    @staticmethod
    def _datetime_to_str(dt: Optional[datetime.datetime]):
        return dt.strftime("%Y-%m-%d %H:%M:%S") if dt else None

    @staticmethod
    def _str_to_datetime(str_: Optional[str]):
        if str_:
            dt = datetime.datetime.strptime(str_, "%Y-%m-%d %H:%M:%S")
            return dt.replace(tzinfo=datetime.timezone.utc)
        else:
            return None

    @validates('start_date', 'end_date', 'creation_date', 'data_uploaded')
    def validate_utc(self, key, datetime_obj):
        return validate_utc(key, datetime_obj)

    def to_dict(self):
        return {
            'transaction_id': self.transaction_id,
            'study_id': self.study_id,
            'patient_id': self.patient_id,
            'name': self.name,
            'birth_date': self.birth_date.strftime("%d/%m/%Y")
            if self.birth_date else None,
            'study_date': self.study_date,
            'patient_consent': self.patient_consent,
            'institution': self.institution,

            'start_date': self._datetime_to_str(self.start_date),
            'end_date': self._datetime_to_str(self.end_date),
            'creation_date': self._datetime_to_str(self.creation_date),

            'version': self.version,
            'analysis_type': self.analysis_type,
            'qa_score': self.qa_score,
            'product_id': self.product_id,

            'task_state': self.task_state.name if self.task_state else None,
            'processing_state': self.processing_state,
            'task_progress': self.task_progress,
            'task_skipped': self.task_skipped,
            'task_cancelled': self.task_cancelled,
            'archived': self.archived,
            'error': self.error,
            'status': self.status,
            'sequences': self.sequences,
            'last_message': self.last_message,

            'data_uploaded': self._datetime_to_str(self.data_uploaded),
            'billable': self.billable,
            'priority': self.priority
        }

    def read_dict(self, d: dict):
        """Read transaction from dictionary"""
        self.transaction_id = d.get('transaction_id')

        self.study_id = d.get('study_id')
        self.patient_id = d.get('patient_id')
        self.name = d.get('name')
        birth_date = d.get('birth_date')
        self.birth_date = datetime.datetime.strptime(
            birth_date, "%d/%m/%Y") if birth_date else None
        self.study_date = d.get('study_date')
        self.patient_consent = d.get('patient_consent')
        self.institution = d.get('institution')

        self.start_date = self._str_to_datetime(d.get('start_date'))
        self.end_date = self._str_to_datetime(d.get('end_date'))
        self.creation_date = self._str_to_datetime(d.get('creation_date'))

        self.version = d.get("version")
        self.analysis_type = d.get("analysis_type")
        self.qa_score = d.get("qa_score")
        self.product_id = d.get('product_id')

        self.task_state = TaskState[
            d.get('task_state')] if d.get('task_state') else None
        self.processing_state = d.get('processing_state')
        self.task_progress = d.get('task_progress')
        self.task_skipped = d.get('task_skipped')
        self.task_cancelled = d.get('task_cancelled')
        self.archived = d.get('archived')
        self.error = d.get('error')
        self.status = d.get('status')
        self.sequences = d.get('sequences')
        self.last_message = d.get('last_message')

        self.data_uploaded = self._str_to_datetime(d.get('data_uploaded'))
        self.billable = d.get('billable')
        self.priority = d.get('priority')

        return self

    def __repr__(self):
        return ("<Transaction("
                "transaction_id='{}',"
                " patient_id='{}',"
                " start_date='{}')>".format(self.transaction_id,
                                            self.patient_id,
                                            self.start_date))


class StudiesMetadata(Base):

    __tablename__ = 'studies_metadata'

    # study instance uid dicom tag
    study_id = Column(String(255), primary_key=True)
    # dicom_grazer, longitudinal_grazer, ...
    origin = Column(String(255))
    # if auto_pull
    c_move_time = Column(DateTime())

    @validates('c_move_time')
    def validate_utc(self, key, datetime_obj):
        return validate_utc(key, datetime_obj)

    def to_dict(self):
        return {'study_id': self.study_id,
                'origin': self.origin,
                'c_move_time': Transaction._datetime_to_str(self.c_move_time)}


class User(Base):

    """for multi-tenant pipelines, users might be required"""
    __tablename__ = 'users'

    # user id
    id = Column(Integer, Sequence('id'), primary_key=True)
    # user name
    name = Column(String(255), unique=True)
    hashed_password = Column(String(128))
    # Datetime the user was added
    added = Column(
        DateTime(),
        default=lambda: datetime.datetime.now(datetime.timezone.utc)
    )

    @validates('added')
    def validate_utc(self, key, datetime_obj):
        return validate_utc(key, datetime_obj)

    @staticmethod
    def password_hash(password):
        return pwd_context.hash(password)

    def verify_password(self, password):
        return pwd_context.verify(password, self.hashed_password)

    def to_dict(self):
        return {'id': self.id,
                'name': self.name,
                'hashed_password': self.hashed_password,
                'added': self.added.strftime("%Y-%m-%d %H:%M:%S")}

    def read_dict(self, d: dict):
        self.id = d.get('id')
        self.name = d.get('name')
        self.hashed_password = d.get('hashed_password')
        added = d.get('added')
        self.added = datetime.datetime.strptime(
            added, "%Y-%m-%d %H:%M:%S") if added else None
        return self


class UserTransaction(Base):

    """for multi-tenant pipelines, transactions might be associated w/ users"""
    __tablename__ = 'users_transactions'

    user_id = Column(Integer, ForeignKey('users.id'),
                     primary_key=True)
    transaction_id = Column(Integer, ForeignKey('transactions.transaction_id'),
                            primary_key=True)

    def to_dict(self):
        return {'user_id': self.user_id,
                'transaction_id': self.transaction_id}

    def read_dict(self, d: dict):
        self.user_id = d.get('user_id')
        self.transaction_id = d.get('transaction_id')
        return self


class UserRole(Base):

    """for multi-tenant pipelines, users might have different roles in the
    associated platform"""
    __tablename__ = 'users_roles'

    user_id = Column(Integer, ForeignKey('users.id'),
                     primary_key=True)
    role_id = Column(String(64), ForeignKey('roles.role_id'),
                     primary_key=True)

    def to_dict(self):
        return {'user_id': self.user_id,
                'role_id': self.role_id}

    def read_dict(self, d: dict):
        self.user_id = d.get('user_id')
        self.role_id = d.get('role_id')
        return self


class UserPreferences(Base):

    """for multi-tenant pipelines, users might have different preferences
    like the language they want their reports in"""
    __tablename__ = 'users_preferences'

    user_id = Column(Integer, ForeignKey('users.id'),
                     primary_key=True)
    report_language = Column(String(255))

    def to_dict(self):
        return {'user_id': self.user_id,
                'report_language': self.report_language}

    def read_dict(self, d: dict):
        self.user_id = d.get('user_id')
        self.report_language = d.get('report_language')
        return self


class Role(Base):

    """for multi-tenant pipelines, users might have different roles in the
    associated platform"""
    __tablename__ = 'roles'

    role_id = Column(String(64),
                     primary_key=True)
    # Description of the role: ['admin', 'default_role', 'spectator' ...]
    description = Column(String)
    # encoded permissions for this role, 1 bit for each
    permissions = Column(Integer)

    def to_dict(self):
        return {'role_id': self.user_id}

    def read_dict(self, d: dict):
        self.role_id = d.get('role_id')
        return self


class SchemaVersion(Base):

    __tablename__ = 'schema_version'

    schema = Column(String(255), primary_key=True,
                    default=constants.TRANSACTIONS_DB_SCHEMA_NAME)
    schema_version = Column(Integer,
                            default=constants.TRANSACTIONS_DB_SCHEMA_VERSION)


def create_all(engine):
    Base.metadata.create_all(bind=engine)
