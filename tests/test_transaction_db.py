import unittest
import tempfile
import shutil
import json
import os
import types
import sys
import traceback
from datetime import datetime, date, timezone, timedelta

from sqlite3 import OperationalError as Sqlite3OperationalError
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.sql import sqltypes

from mediaire_toolbox.transaction_db.transaction_db import (TransactionDB,
                                                            utcnow)
from mediaire_toolbox.transaction_db.model import (
    TaskState, Transaction, UserTransaction, User, Role, UserRole,
    StudiesMetadata, Site, UserSite
)
from mediaire_toolbox.transaction_db.exceptions import TransactionDBException

from temp_db_base import TempDBFactory

temp_db = TempDBFactory('test_transaction_db')


class TestTransactionDB(unittest.TestCase):

    @classmethod
    def tearDownClass(self):
        temp_db.delete_temp_folder()

    def _get_test_transaction(self):
        t = Transaction()
        # we only need to fill metadata before creating a new transaction
        t.name = 'Pere'
        t.patient_id = '1'
        t.study_id = 'S1'
        t.birth_date = datetime(1982, 10, 29)
        return t

    def test_multi_session(self):
        engine = temp_db.get_temp_db()
        # no db created yet
        t = Transaction()
        t.name = 'Pere'

        t_db_1 = TransactionDB(engine, False)
        _ = TransactionDB(engine)
        t_db_1.create_transaction(t)
        t_1 = t_db_1.get_transaction(1)
        self.assertEqual('Pere', t_1.name)

    def test_read_transaction_from_dict(self):
        d = {
            'transaction_id': 1, 'name': 'John Doe',
            'birth_date': '01/02/2020'}
        t = Transaction().read_dict(d)
        engine = temp_db.get_temp_db()
        t_db = TransactionDB(engine)
        t_id = t_db.create_transaction(t)
        t_from_db = t_db.get_transaction(t_id)
        self.assertEqual(d['transaction_id'], t_from_db.transaction_id)
        self.assertEqual(d['name'], t_from_db.name)
        self.assertEqual(date(2020, 2, 1), t_from_db.birth_date)

    def test_read_dict_dates(self):
        # test that date and datetimes are parsed correctly
        engine = temp_db.get_temp_db()
        t_db = TransactionDB(engine)
        datetime_vars = ['start_date', 'end_date', 'data_uploaded',
                         'patient_consent_date', 'creation_date']
        date_vars = ['birth_date']
        t = Transaction()
        test_datetime = datetime(2020, 2, 1, 18, 30, 4, tzinfo=timezone.utc)
        for key in datetime_vars:
            setattr(t, key, test_datetime)
        for key in date_vars:
            setattr(t, key, datetime(2020, 2, 1))

        t_r = Transaction().read_dict(t.to_dict())
        t_id = t_db.create_transaction(t_r)
        t_r_from_db = t_db.get_transaction(t_id)
        self.assertEqual(test_datetime.replace(tzinfo=None),
                         t_r_from_db.start_date)
        self.assertEqual(test_datetime.replace(tzinfo=None),
                         t_r_from_db.end_date)
        self.assertEqual(date(2020, 2, 1), t_r_from_db.birth_date)

    def test_read_dict_task_state(self):
        # test that task state is parsed correctly
        t = Transaction()
        t.task_state = TaskState.completed
        t_r = Transaction().read_dict(t.to_dict())
        self.assertEqual(TaskState.completed, t_r.task_state)

    def test_read_dict_patient_consent(self):
        """Test that legacy patient_consent is parsed correctly."""
        t = Transaction()
        t_dict = t.to_dict()

        with self.subTest(patient_consent=1,
                          patient_consent_date=None,
                          creation_date=None):
            t_dict['patient_consent_date'] = None
            t_dict['patient_consent'] = 1
            t_r = Transaction().read_dict(t_dict)
            self.assertEqual(t_r.patient_consent, 1)
            self.assertIsNotNone(t_r.patient_consent_date)

        now = utcnow().replace(microsecond=0)
        nowstr = datetime.strftime(now, "%Y-%m-%d %H:%M:%S")
        with self.subTest(patient_consent=1,
                          patient_consent_date=None,
                          creation_date=now):
            t_dict['creation_date'] = nowstr
            t_r = Transaction().read_dict(t_dict)
            self.assertEqual(t_r.patient_consent, 1)
            self.assertEqual(t_r.patient_consent_date, now)

        with self.subTest(patient_consent=1,
                          patient_consent_date=now,
                          creation_date=None):
            del t_dict['creation_date']
            t_dict['patient_consent_date'] = nowstr
            t_r = Transaction().read_dict(t_dict)
            self.assertEqual(t_r.patient_consent, 1)
            self.assertEqual(t_r.patient_consent_date, now)

    def test_read_transaction_from_dict_is_complete(self):
        """get all variables of transaction except non generic types

        Test that these variables are read from the deserialization function.
        """
        non_generic_vars = [
            'start_date', 'end_date', 'birth_date', 'task_state',
            'data_uploaded', 'creation_date', 'patient_consent',
            'patient_consent_date']
        CALLABLES = types.FunctionType, types.MethodType
        var = [
            key for key, value in Transaction.__dict__.items()
            if not isinstance(value, CALLABLES)]
        var = [
            key for key in var
            if key[0] != '_' and key not in non_generic_vars]

        t = Transaction()
        counter = 0
        for key in var:
            counter += 1
            setattr(t, key, counter)

        t2 = Transaction()
        t2.read_dict(t.to_dict())
        counter = 0
        for key in var:
            counter += 1
            with self.subTest(key=key):
                self.assertEqual(counter, getattr(t2, key))

    def test_create_transaction_index_sequences(self):
        engine = temp_db.get_temp_db()
        tr_1 = self._get_test_transaction()
        tr_1.last_message = json.dumps({
            'data': {
                'dicom_info': {
                    't1': {'header': {'SeriesDescription': 'series_t1_1'}},
                    't2': {'header': {'SeriesDescription': 'series_t2_1'}}}
            }
        })
        t_db = TransactionDB(engine)
        t_id = t_db.create_transaction(tr_1)
        tr_2 = t_db.get_transaction(t_id)

        self.assertEqual('series_t1_1;series_t2_1',
                         tr_2.sequences)

    def test_create_transaction_lm(self):
        engine = temp_db.get_temp_db()
        tr_1 = self._get_test_transaction()
        tr_1.last_message = json.dumps({
            't_id': None,
            'data': {
                'dicom_info': {
                    't1': {'header': {'SeriesDescription': 'series_t1_1'}},
                    't2': {'header': {'SeriesDescription': 'series_t2_1'}}}
            }
        })
        t_db = TransactionDB(engine)
        t_id = t_db.create_transaction(tr_1)
        tr_2 = t_db.get_transaction(t_id)

        self.assertEqual(t_id, json.loads(tr_2.last_message)['t_id'])

    def test_get_transaction(self):
        engine = temp_db.get_temp_db()
        tr_1 = self._get_test_transaction()

        t_db = TransactionDB(engine)
        t_id = t_db.create_transaction(tr_1)
        # the engine returns the ID of the newly created transaction
        tr_2 = t_db.get_transaction(t_id)

        self.assertEqual(tr_1.name, tr_2.name)
        self.assertEqual(tr_1.patient_id, tr_2.patient_id)
        self.assertEqual(tr_1.study_id, tr_2.study_id)
        self.assertFalse(tr_2.start_date)
        self.assertEqual(t_id, tr_2.transaction_id)
        self.assertEqual(tr_2.task_state, TaskState.queued)

        t_db.close()

    def test_set_creation_date(self):
        engine = temp_db.get_temp_db()
        tr_1 = self._get_test_transaction()
        t_db = TransactionDB(engine)
        t_id = t_db.create_transaction(tr_1)
        tr_2 = t_db.get_transaction(t_id)
        self.assertTrue(tr_2.creation_date)

    def test_set_start_date(self):
        # set start date at first processing
        engine = temp_db.get_temp_db()
        tr_1 = self._get_test_transaction()

        t_db = TransactionDB(engine)
        t_id = t_db.create_transaction(tr_1)
        # the engine returns the ID of the newly created transaction
        tr_2 = t_db.get_transaction(t_id)

        self.assertFalse(tr_2.start_date)

        t_db.set_processing(t_id, '', '')
        tr_3 = t_db.get_transaction(t_id)
        self.assertTrue(tr_3.start_date)

        t_db.set_processing(t_id, '', '')
        tr_4 = t_db.get_transaction(t_id)
        self.assertEqual(tr_3.start_date, tr_4.start_date)

        t_db.close()

    def test_change_processing_state(self):
        engine = temp_db.get_temp_db()
        tr_1 = self._get_test_transaction()

        t_db = TransactionDB(engine)
        t_id = t_db.create_transaction(tr_1)
        # called when a transaction changes its processing state
        t_db.set_processing(t_id, 'spm_volumetry', '{}', 10)
        t = t_db.get_transaction(t_id)

        self.assertEqual(t.processing_state, 'spm_volumetry')
        self.assertEqual(t.task_state, TaskState.processing)
        self.assertEqual(t.task_progress, 10)

        t_db.close()

    def test_transaction_failed(self):
        engine = temp_db.get_temp_db()
        tr_1 = self._get_test_transaction()

        t_db = TransactionDB(engine)
        t_id = t_db.create_transaction(tr_1)
        t_db.set_processing(t_id, '', '')
        # to be called when a transaction fails
        t_db.set_failed(t_id, 'because it failed')
        t = t_db.get_transaction(t_id)

        self.assertEqual(t.task_state, TaskState.failed)
        self.assertTrue(t.end_date > t.start_date)
        self.assertEqual(t.error, 'because it failed')

        t_db.close()

    def test_transaction_completed(self):
        engine = temp_db.get_temp_db()
        tr_1 = self._get_test_transaction()

        t_db = TransactionDB(engine)
        t_id = t_db.create_transaction(tr_1)
        t_db.set_processing(t_id, '', '')
        # to be called when a transaction completes
        t_db.set_completed(t_id)
        t = t_db.get_transaction(t_id)

        self.assertEqual(t.task_state, TaskState.completed)
        self.assertEqual(t.status, 'unseen')
        self.assertTrue(t.end_date > t.start_date)

        t_db.close()

    def test_transaction_completed_end_date_immutable(self):
        engine = temp_db.get_temp_db()
        tr_1 = self._get_test_transaction()

        t_db = TransactionDB(engine)
        t_id = t_db.create_transaction(tr_1)
        t = t_db.get_transaction(t_id)
        # check that end date is None on creation
        self.assertFalse(t.end_date)
        # complete transaction
        t_db.set_completed(t_id)
        t = t_db.get_transaction(t_id)

        # reset to processing and then to complete again
        end_date_1 = t.end_date
        t_db.set_processing(t_id, '', '')
        t_db.set_completed(t_id)
        t = t_db.get_transaction(t_id)
        self.assertEqual(end_date_1, t.end_date)

        t_db.close()

    def test_transaction_archived(self):
        engine = temp_db.get_temp_db()
        tr_1 = self._get_test_transaction()

        t_db = TransactionDB(engine)
        t_id = t_db.create_transaction(tr_1)
        t = t_db.get_transaction(t_id)
        self.assertEqual(t.archived, 0)
        t_db.set_archived(t_id)
        t = t_db.get_transaction(t_id)
        self.assertEqual(t.archived, 1)
        t_db.close()

    def test_peek_queued(self):
        engine = temp_db.get_temp_db()
        t_db = TransactionDB(engine)

        tr_1 = self._get_test_transaction()
        t_id_1 = t_db.create_transaction(tr_1)
        t_db.set_queued(t_id_1, '', 'wait')

        tr_2 = self._get_test_transaction()
        # just in case
        t_id_2 = t_db.create_transaction(tr_2)

        t_db.set_queued(t_id_2, '', 'wait')

        self.assertFalse(t_db.peek_queued('non_wait'))

        t = t_db.peek_queued('wait')
        self.assertEqual(t.transaction_id, t_id_1)
        t = t_db.peek_queued('wait')
        self.assertEqual(t.transaction_id, t_id_1)

        t_db.set_processing(t_id_1, 'spm', '', 50)

        t = t_db.peek_queued('wait')
        self.assertEqual(t.transaction_id, t_id_2)

        t_db.close()

    def test_peek_queued_all(self):
        engine = temp_db.get_temp_db()
        t_db = TransactionDB(engine)

        tr_1 = self._get_test_transaction()
        t_id_1 = t_db.create_transaction(tr_1)
        t_db.set_queued(t_id_1, '', 'wait')

        tr_2 = self._get_test_transaction()
        # just in case
        t_id_2 = t_db.create_transaction(tr_2)

        t_db.set_queued(t_id_2, '', 'wait')

        lt = list(t_db.peek_queued('wait', peek_all=True))
        self.assertEqual(2, len(lt))
        self.assertEqual([tr_1, tr_2], lt)

        t_db.close()

    def test_set_status(self):
        engine = temp_db.get_temp_db()
        tr_1 = self._get_test_transaction()

        t_db = TransactionDB(engine)
        t_id = t_db.create_transaction(tr_1)

        t_db.set_status(t_id, 'reviewed')
        t = t_db.get_transaction(t_id)

        self.assertEqual(t.status, 'reviewed')

        t_db.close()

    def test_transaction_skipped(self):
        engine = temp_db.get_temp_db()
        tr_1 = self._get_test_transaction()

        t_db = TransactionDB(engine)
        t_id = t_db.create_transaction(tr_1)

        # to be called when a transaction is skipped
        t_db.set_skipped(t_id, 'because it is skipped')
        t = t_db.get_transaction(t_id)

        self.assertEqual(t.task_skipped, 1)
        self.assertEqual(t.error, 'because it is skipped')

        t_db.close()

    def test_transaction_cancelled(self):
        engine = temp_db.get_temp_db()
        tr_1 = self._get_test_transaction()

        t_db = TransactionDB(engine)
        t_id = t_db.create_transaction(tr_1)

        # to be called when a transaction is skipped
        t_db.set_cancelled(t_id, 'because it is cancelled')
        t = t_db.get_transaction(t_id)

        self.assertEqual(t.task_cancelled, 1)
        self.assertEqual(t.error, 'because it is cancelled')

        t_db.close()

    def test_change_last_message(self):
        engine = temp_db.get_temp_db()
        tr_1 = self._get_test_transaction()

        t_db = TransactionDB(engine)
        t_id = t_db.create_transaction(tr_1)

        # update last_message field
        t_db.set_last_message(t_id, 'last_message')
        t = t_db.get_transaction(t_id)

        self.assertEqual(t.last_message, 'last_message')

        t_db.close()

    @unittest.expectedFailure
    def test_fail_on_get_non_existing_transaction(self):
        engine = temp_db.get_temp_db()
        t_db = TransactionDB(engine)
        t_db.get_transaction(1)

    def test_transaction_with_user_id(self):
        engine = temp_db.get_temp_db()
        tr_1 = self._get_test_transaction()

        t_db = TransactionDB(engine)
        user_id = t_db.add_user('Pere', 'pwd')
        t_id = t_db.create_transaction(tr_1, user_id)

        t = t_db.get_transaction(t_id)
        self.assertNotEqual(None, t)

        ut = t_db.session.query(UserTransaction) \
            .filter_by(transaction_id=t.transaction_id) \
            .filter_by(user_id=user_id).first()

        self.assertEqual(ut.user_id, user_id)
        self.assertEqual(t.transaction_id, ut.transaction_id)

        t_db.close()

    def test_transaction_with_product_id(self):
        engine = temp_db.get_temp_db()
        tr_1 = self._get_test_transaction()
        tr_2 = self._get_test_transaction()

        t_db = TransactionDB(engine)
        t_id = t_db.create_transaction(tr_1, product_id=1)
        t_id_2 = t_db.create_transaction(tr_2, product_id=2)

        t = t_db.get_transaction(t_id)
        self.assertNotEqual(None, t)
        self.assertEqual(1, t.product_id)

        t2 = t_db.get_transaction(t_id_2)
        self.assertNotEqual(None, t2)
        self.assertEqual(2, t2.product_id)

        t_db.close()

    def test_migrations(self):
        temp_folder = tempfile.mkdtemp(
            suffix='_test_migrations_transaction_db_')
        temp_db_path = os.path.join(temp_folder, 't_v1.db')
        shutil.copy('tests/fixtures/t_v1.db', temp_db_path)
        engine = create_engine('sqlite:///' + temp_db_path)
        # should execute all migrations code
        t_db = TransactionDB(engine, create_db=True, db_file_path=temp_db_path)

        self.assertTrue(os.path.exists(temp_db_path + '.v_1.bkp'))
        # add a new transaction with the current model
        t = Transaction()
        t_db.create_transaction(t)
        shutil.rmtree(temp_folder)

    def test_json_serialization(self):
        t = self._get_test_transaction()
        t.task_state = TaskState.completed
        t.start_date = utcnow()
        t.end_date = utcnow()
        print(t.to_dict()['task_state'])
        print(t.to_dict()['task_state'] == 'completed')
        self.assertTrue(t.to_dict()['task_state'] == 'completed')
        json.dumps(t.to_dict())

    def test_retry_logic(self):
        """test that our database retry logic works.
        Raise exception randomly and perform the given task randomly,
        such that retrying should eventually work"""
        engine = temp_db.get_temp_db()
        tr_1 = self._get_test_transaction()

        t_db = TransactionDB(engine)
        t_id = t_db.create_transaction(tr_1)
        t = t_db.get_transaction(t_id)
        self.assertEqual(0, t.patient_consent)

        orig_f = t_db._get_transaction_or_raise_exception

        should_fail_once = True

        def mocked_f(t_id):
            nonlocal should_fail_once
            if should_fail_once:
                should_fail_once = False
                # Raising this exception means it should be retried
                raise OperationalError(None, None, None)
            return orig_f(t_id)

        t_db._get_transaction_or_raise_exception = mocked_f

        try:
            t_db.set_patient_consent(t_id)
        except Exception:
            traceback.print_exc(file=sys.stdout)
            pass

        t = t_db.get_transaction(t_id)
        self.assertEqual(1, t.patient_consent)

    def test_retry_logic_2(self):
        """test that our database retry logic works.
        Raise exception randomly and perform the given task randomly,
        such that retrying should eventually work"""
        engine = temp_db.get_temp_db()
        tr_1 = self._get_test_transaction()

        t_db = TransactionDB(engine)
        t_id = t_db.create_transaction(tr_1)
        t = t_db.get_transaction(t_id)
        self.assertEqual(0, t.patient_consent)

        orig_f = t_db._get_transaction_or_raise_exception

        should_fail_once = True

        def mocked_f(t_id):
            nonlocal should_fail_once
            if should_fail_once:
                should_fail_once = False
                # Raising this exception means it should be retried
                raise Sqlite3OperationalError
            return orig_f(t_id)

        t_db._get_transaction_or_raise_exception = mocked_f

        try:
            t_db.set_patient_consent(t_id)
        except Exception:
            pass

        t = t_db.get_transaction(t_id)
        self.assertEqual(1, t.patient_consent)

    def test_set_patient_consent(self):
        engine = temp_db.get_temp_db()
        tr_1 = self._get_test_transaction()

        t_db = TransactionDB(engine)
        t_id = t_db.create_transaction(tr_1)
        t = t_db.get_transaction(t_id)
        self.assertEqual(0, t.patient_consent)

        # set patient consent
        with self.subTest(consent=True):
            t_db.set_patient_consent(t_id)
            t = t_db.get_transaction(t_id)
            self.assertEqual(1, t.patient_consent)

        # unset patient consent
        with self.subTest(consent=True):
            t_db.unset_patient_consent(t_id)
            t = t_db.get_transaction(t_id)
            self.assertEqual(0, t.patient_consent)
        t_db.close()

    def test_set_patient_consent_date(self):
        """Test that `patient_consent_date` can be set."""
        engine = temp_db.get_temp_db()
        t_db = TransactionDB(engine)
        t_id = t_db.create_transaction(self._get_test_transaction())
        t = t_db.get_transaction(t_id)
        self.assertEqual(0, t.patient_consent)

        # set patient consent
        with self.subTest(consent=True, consent_date=None):
            t_db.set_patient_consent(t_id)
            t = t_db.get_transaction(t_id)
            self.assertEqual(1, t.patient_consent)
            self.assertLess(t.patient_consent_date - utcnow(),
                            timedelta(seconds=1))

        # unset patient consent
        with self.subTest(consent=False, consent_date=None):
            t_db.unset_patient_consent(t_id)
            t = t_db.get_transaction(t_id)
            self.assertEqual(0, t.patient_consent)
            self.assertIsNone(t.patient_consent_date)

        # set patient consent to specific date
        with self.subTest(consent=True, consent_date=None):
            now = utcnow()
            t_db.set_patient_consent(t_id, now)
            t = t_db.get_transaction(t_id)
            self.assertEqual(1, t.patient_consent)
            self.assertEqual(t.patient_consent_date, now)

        t_db.close()

    def test_set_qa_score(self):
        engine = temp_db.get_temp_db()
        tr_1 = self._get_test_transaction()

        t_db = TransactionDB(engine)
        t_id = t_db.create_transaction(tr_1, qa_score='good')
        t = t_db.get_transaction(t_id)
        self.assertEqual('good', t.qa_score)

        t_db.set_qa_score(t_id, 'rejected')
        t = t_db.get_transaction(t_id)
        self.assertEqual('rejected', t.qa_score)
        t_db.close()

    def test_set_billable(self):
        engine = temp_db.get_temp_db()
        tr_1 = self._get_test_transaction()

        t_db = TransactionDB(engine)
        t_id = t_db.create_transaction(tr_1)
        t = t_db.get_transaction(t_id)
        self.assertEqual(None, t.billable)
        # set billable
        t_db.set_billable(t_id, 'bill')
        t = t_db.get_transaction(t_id)
        self.assertEqual('bill', t.billable)
        t_db.close()

    def test_set_priority(self):
        engine = temp_db.get_temp_db()
        tr_1 = self._get_test_transaction()

        t_db = TransactionDB(engine)
        t_id = t_db.create_transaction(tr_1)
        t = t_db.get_transaction(t_id)
        self.assertEqual(0, t.priority)

        t_db.set_priority(t_id, 2)
        t = t_db.get_transaction(t_id)
        self.assertEqual(2, t.priority)

        t_db.close()

    def test_patient_consent_hybrid_property_filter(self):
        """Test that filtering by hybrid properties works."""
        engine = temp_db.get_temp_db()
        t_db = TransactionDB(engine)

        tr_no = self._get_test_transaction()
        t_id_no = t_db.create_transaction(tr_no)
        t_no = t_db.get_transaction(t_id_no)
        self.assertEqual(t_no.patient_consent, 0)
        self.assertIsNone(t_no.patient_consent_date)

        tr_yes = self._get_test_transaction()
        tr_yes.patient_consent = 1
        t_id_yes = t_db.create_transaction(tr_yes)
        t_yes = t_db.get_transaction(t_id_yes)
        self.assertEqual(t_yes.patient_consent, 1)
        self.assertIsNotNone(t_yes.patient_consent_date)

        with self.subTest(patient_consent=1):
            ts = (t_db.session
                  .query(Transaction)
                  .filter(Transaction.patient_consent == 1))
            self.assertCountEqual(list(ts), [t_yes])

        with self.subTest(patient_consent=0):
            ts = (t_db.session
                  .query(Transaction)
                  .filter(Transaction.patient_consent == 0))
            self.assertCountEqual(list(ts), [t_no])

        with self.subTest(patient_consent_date=' != None'):
            ts = (t_db.session
                  .query(Transaction)
                  .filter(Transaction.patient_consent_date != None))  # noqa: E711,E501
            self.assertCountEqual(list(ts), [t_yes])

        with self.subTest(patient_consent_date=None):
            ts = (t_db.session
                  .query(Transaction)
                  .filter(Transaction.patient_consent_date == None))  # noqa: E711,E501
            self.assertCountEqual(list(ts), [t_no])

    def test_add_user_ok(self):
        """test that we can add User entity"""
        engine = temp_db.get_temp_db()
        t_db = TransactionDB(engine)
        user_id = t_db.add_user('Pere', 'pwd')
        user = t_db.session.query(User).get(user_id)
        self.assertEqual('Pere', user.name)
        self.assertTrue(user.hashed_password)

    def test_remove_user_ok(self):
        """test that we can remove an existing user from the database"""
        engine = temp_db.get_temp_db()
        t_db = TransactionDB(engine)
        user_id = t_db.add_user('Pere', 'pwd')
        user = t_db.session.query(User).get(user_id)
        self.assertEqual('Pere', user.name)
        t_db.remove_user(user_id)
        user = t_db.session.query(User).get(user_id)
        self.assertFalse(user)

    @unittest.expectedFailure
    def test_add_user_already_exists(self):
        """test that we can't add duplicate users by user name"""
        engine = temp_db.get_temp_db()
        t_db = TransactionDB(engine)
        user_id = t_db.add_user('Pere', 'pwd')
        self.assertTrue(user_id >= 0)
        t_db.add_user('Pere', 'pwd')

    def test_add_role_ok(self):
        """test that we can add a Role entity"""
        engine = temp_db.get_temp_db()
        t_db = TransactionDB(engine)
        t_db.add_role('radiologist', 'whatever', 128)
        role = t_db.session.query(Role).get('radiologist')
        self.assertEqual('whatever', role.description)
        self.assertEqual(128, role.permissions)

    @unittest.expectedFailure
    def test_add_role_already_exists(self):
        """test that we can't add the same Role twice"""
        engine = temp_db.get_temp_db()
        t_db = TransactionDB(engine)
        t_db.add_role('radiologist', 'whatever')
        t_db.add_role('radiologist', 'whatever')

    def __user_has_role(self, t_db, user_id, role_id):
        return t_db.session.query(UserRole).filter_by(user_id=user_id)\
                                           .filter_by(role_id=role_id)\
                                           .first()

    def test_add_user_role_ok(self):
        """test that we can assign a role to a user"""
        engine = temp_db.get_temp_db()
        t_db = TransactionDB(engine)
        user_id = t_db.add_user('Pere', 'pwd')
        t_db.add_role('radiologist', 'whatever', 128)

        t_db.add_user_role(user_id, 'radiologist')

        self.assertTrue(self.__user_has_role(t_db, user_id, 'radiologist'))

    @unittest.expectedFailure
    def test_add_user_role_fail_on_non_existing_role(self):
        engine = temp_db.get_temp_db()
        t_db = TransactionDB(engine)
        user_id = t_db.add_user('Pere', 'pwd')

        t_db.add_user_role(user_id, 'radiologist')

    @unittest.expectedFailure
    def test_add_user_role_fail_on_non_existing_user(self):
        engine = temp_db.get_temp_db()
        t_db = TransactionDB(engine)
        t_db.add_role('radiologist', 'whatever')

        t_db.add_user_role(1, 'radiologist')

    @unittest.expectedFailure
    def test_add_user_role_already_exists(self):
        """test that we can't assign twice the same role to a user"""
        engine = temp_db.get_temp_db()
        t_db = TransactionDB(engine)
        user_id = t_db.add_user('Pere', 'pwd')
        t_db.add_role('radiologist', 'whatever')

        t_db.add_user_role(user_id, 'radiologist')
        t_db.add_user_role(user_id, 'radiologist')

    def test_revoke_user_role_ok(self):
        """test that we can revoke a role from a user"""
        engine = temp_db.get_temp_db()
        t_db = TransactionDB(engine)
        user_id = t_db.add_user('Pere', 'pwd')
        t_db.add_role('radiologist', 'whatever', 128)

        t_db.add_user_role(user_id, 'radiologist')
        self.assertTrue(self.__user_has_role(t_db, user_id, 'radiologist'))
        t_db.revoke_user_role(user_id, 'radiologist')
        self.assertFalse(self.__user_has_role(t_db, user_id, 'radiologist'))

    @unittest.expectedFailure
    def test_revoke_user_role_didnt_exist(self):
        """test that an Exception is thrown if we want to revoke an
        already revoked role from a user"""
        engine = temp_db.get_temp_db()
        t_db = TransactionDB(engine)
        user_id = t_db.add_user('Pere', 'pwd')
        t_db.add_role('radiologist', 'whatever')

        t_db.revoke_user_role(user_id, 'radiologist')

    def test_user_preferences(self):
        """test that the preferences saving and retrieving system works"""
        engine = temp_db.get_temp_db()
        t_db = TransactionDB(engine)
        user_id_1 = t_db.add_user('Pere1', 'pwd')
        user_id_2 = t_db.add_user('Pere2', 'pwd')

        self.assertTrue(t_db.get_user_preferences(user_id_1) is None)
        self.assertTrue(t_db.get_user_preferences(user_id_2) is None)

        preferences = {'report_language': 'en'}
        t_db.set_user_preferences(user_id_1, preferences)
        preferences = {'report_language': 'de'}
        t_db.set_user_preferences(user_id_2, preferences)

        prefs = t_db.get_user_preferences(user_id_1)
        self.assertEqual(user_id_1, prefs['user_id'])
        self.assertEqual('en', prefs['report_language'])

        prefs = t_db.get_user_preferences(user_id_2)
        self.assertEqual(user_id_2, prefs['user_id'])
        self.assertEqual('de', prefs['report_language'])

    # TODO
    @unittest.skip("Key validation does not work for whatever reason.")
    def test_user_preferences_invalid_keys(self):
        """Test that invalid preference key raises `TransactionDBException`."""
        engine = temp_db.get_temp_db()
        t_db = TransactionDB(engine)
        user_id_1 = t_db.add_user('Pere1', 'pwd')

        with self.assertRaises(TransactionDBException):
            t_db.set_user_preferences(user_id_1, {'foo': 'bar'})

    # TODO
    @unittest.skip("Key validation does not work for whatever reason.")
    @unittest.expectedFailure
    def test_user_preferences_invalid_user(self):
        """Test that `user_id` can't be set arbitrarily as it is a foreign key.

        test that user_id is a foreign key in the relational model of the user
        preferences, and as such will cause the db to fail if we provide
        whatever as user_id
        """
        engine = temp_db.get_temp_db()
        t_db = TransactionDB(engine)

        t_db.set_user_preferences(100, {'report_language': 'en'})

    def test_add_get_study_metadata(self):
        engine = temp_db.get_temp_db()
        t_db = TransactionDB(engine)

        t_db.add_study_metadata('s1', 'dicom_grazer', utcnow())
        md = t_db.get_study_metadata('s1')

        self.assertEqual('dicom_grazer', md.origin)

        md = t_db.get_study_metadata('s2')

        self.assertTrue(md is None)

    @unittest.expectedFailure
    def test_study_metadata_immutable(self):
        engine = temp_db.get_temp_db()
        t_db = TransactionDB(engine)

        t_db.add_study_metadata('s1', 'dicom_grazer', utcnow())
        t_db.add_study_metadata('s1', 'longitudinal_grazer', utcnow())

    def test_study_metadata_mutable(self):
        engine = temp_db.get_temp_db()
        t_db = TransactionDB(engine)

        t_db.add_study_metadata('s1', 'dicom_grazer', utcnow())
        t_db.add_study_metadata('s1', 'longitudinal_grazer', utcnow(),
                                overwrite=True)

        md = t_db.get_study_metadata('s1')

        self.assertEqual('longitudinal_grazer', md.origin)

    def test_study_metadata_to_dict(self):
        md = StudiesMetadata()
        md.origin = 'dicom_grazer'
        md.c_move_time = utcnow()
        md.study_id = 's1'

        self.assertEqual({
            'origin': 'dicom_grazer',
            'c_move_time': Transaction._datetime_to_str(md.c_move_time),
            'study_id': 's1'
        }, md.to_dict())

    def test_datetime_utc_validation_accept(self):
        """Checks that DateTime fields in all tables accept UTC values."""
        for TableClass in [Transaction, UserTransaction, User, Role,
                           UserRole, StudiesMetadata]:
            row = TableClass()
            for column in row.__table__.columns:
                if type(column.type) != sqltypes.DateTime:
                    continue
                with self.subTest(column=str(column)):
                    setattr(row, column.name, datetime.now(timezone.utc))

    def test_datetime_utc_validation_reject(self):
        """Checks that DateTime fields in all tables reject non-UTC values."""
        for TableClass in [Transaction, UserTransaction, User, Role,
                           UserRole, StudiesMetadata]:
            row = TableClass()
            for column in row.__table__.columns:
                if type(column.type) != sqltypes.DateTime:
                    continue
                with self.subTest(column=str(column)):
                    with self.assertRaises(ValueError):
                        setattr(row, column.name, datetime.now())

    def test_utcnow(self):
        """Checks that `utcnow()` returns aware object in UTC"""
        self.assertEqual(utcnow().tzinfo, timezone.utc)

    def test_site_id(self):
        """Test that transactions are associated with the correct site."""
        engine = temp_db.get_temp_db()
        t_db = TransactionDB(engine)

        tr_default = self._get_test_transaction()
        t_id_default = t_db.create_transaction(tr_default)
        tr_default_created = t_db.get_transaction(t_id_default)
        self.assertEqual(tr_default_created.site_id, tr_default.site_id)

        tr_extra = self._get_test_transaction()
        tr_extra.site_id = 1
        t_id_extra = t_db.create_transaction(tr_extra)
        tr_extra_created = t_db.get_transaction(t_id_extra)
        self.assertEqual(tr_extra_created.site_id, tr_extra.site_id)

    def test_users_sites(self):
        """Test that User <-> Site association works."""
        engine = temp_db.get_temp_db()
        t_db = TransactionDB(engine)

        user_id = t_db.add_user('RaymondDamian', 'pwd')

        site_default = Site(id=0, name='default')
        t_db.session.add(site_default)

        site_extra = Site(id=1, name='extra')
        t_db.session.add(site_extra)

        user_site = UserSite(user_id=user_id, site_id=site_default.id)
        t_db.session.add(user_site)
        t_db.session.commit()

        users_sites = t_db.session.query(UserSite)
        self.assertEqual(len(list(users_sites)), 1)
        user_site = users_sites.first()
        self.assertEqual(user_site.user_id, user_id)
        self.assertEqual(user_site.site_id, site_default.id)

    def test_get_user_sites(self):
        engine = temp_db.get_temp_db()
        t_db = TransactionDB(engine)
        user_id = t_db.add_user('RaymondDamian', 'pwd')
        desired_site_ids = [0, 1]
        for site_id in desired_site_ids:
            site = Site(id=site_id, name='site-{}'.format(site_id))
            t_db.session.add(site)
            user_site = UserSite(user_id=user_id, site_id=site_id)
            t_db.session.add(user_site)
        t_db.session.commit()

        other_user_id = t_db.add_user('NotRaymond', 'pwd')
        disallowed_site = Site(id=2, name='site-2')
        self.assertNotIn(disallowed_site.id, desired_site_ids)
        t_db.session.add(disallowed_site)
        other_user_site = UserSite(user_id=other_user_id,
                                   site_id=disallowed_site.id)
        t_db.session.add(other_user_site)
        t_db.session.commit()

        user_sites = t_db.get_user_sites(user_id)
        user_site_ids = [us.site_id for us in user_sites]

        self.assertCountEqual(user_site_ids, desired_site_ids)

    def test_set_user_sites(self):
        """Test that setting user's sites works."""
        engine = temp_db.get_temp_db()
        t_db = TransactionDB(engine)
        user_id = t_db.add_user('RaymondDamian', 'pwd')
        desired_site_ids = [0, 1]
        for site_id in desired_site_ids:
            site_default = Site(id=site_id, name='site-{}'.format(site_id))
            t_db.session.add(site_default)
        t_db.session.commit()

        t_db.set_user_sites(user_id, desired_site_ids)

        user_sites = t_db.session.query(UserSite)
        self.assertEqual(len(list(user_sites)), len(desired_site_ids))
        for user_site in user_sites:
            self.assertEqual(user_site.user_id, user_id)
            self.assertIn(user_site.site_id, desired_site_ids)

    def test_set_user_sites_clear_existing(self):
        """Test that existing user_sites are cleared when setting new ones."""
        engine = temp_db.get_temp_db()
        t_db = TransactionDB(engine)
        user_id = t_db.add_user('RaymondDamian', 'pwd')
        previous_site_ids = [0, 1]
        for site_id in previous_site_ids:
            site = Site(id=site_id, name='site-{}'.format(site_id))
            t_db.session.add(site)
            user_site = UserSite(user_id=user_id, site_id=site_id)
            t_db.session.add(user_site)
        t_db.session.commit()
        user_sites = t_db.session.query(UserSite)
        self.assertEqual(len(list(user_sites)), len(previous_site_ids))

        t_db.set_user_sites(user_id, [])

        user_sites = t_db.get_user_sites(user_id)
        user_site_ids = [us.site_id for us in user_sites]
        self.assertEqual(user_site_ids, [])
