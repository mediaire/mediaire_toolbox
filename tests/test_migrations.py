import unittest
from unittest.mock import patch
import tempfile
import shutil
import json
import os
from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.schema import MetaData

from mediaire_toolbox.transaction_db import migrations
from mediaire_toolbox.transaction_db.transaction_db import (
    TransactionDB,
    get_transaction_model,
    utcnow
)
from mediaire_toolbox.transaction_db.model import Transaction


class TestMigration(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self.temp_folder = tempfile.mkdtemp(suffix='_test_migration_')

    @classmethod
    def tearDownClass(self):
        shutil.rmtree(self.temp_folder)

    def _get_temp_db(self, test_index):
        return create_engine('sqlite:///' +
                             os.path.join(self.temp_folder,
                                          't' + str(test_index) + '.db') +
                             '?check_same_thread=False')

    def test_migrate_institution(self):
        engine = self._get_temp_db(2)
        t_db = TransactionDB(engine)
        last_message = {
            'data': {
                'dicom_info': {
                    't1': {
                        'header': {
                            'InstitutionName': 'MockInstitution'
                        }}}}}
        tr_1 = Transaction()
        tr_1.last_message = json.dumps(last_message)
        t_id = t_db.create_transaction(tr_1)

        # remove institution field
        session = t_db.session
        tr_2 = t_db.get_transaction(t_id)
        tr_2.institution = ''
        session.commit()
        self.assertEqual('', t_db.get_transaction(t_id).institution)

        # execute migrate python script
        model = get_transaction_model(engine)
        migrations.migrate_institution(session, model)
        session.commit()
        tr_2 = t_db.get_transaction(t_id)
        self.assertEqual('MockInstitution', tr_2.institution)

        t_db.close()

    def test_migrate_sequences(self):
        engine = self._get_temp_db(3)
        t_db = TransactionDB(engine)
        last_message = {
            'data': {
                'dicom_info': {
                    't1': {
                        'header': {
                            'SeriesDescription': 'T1_sequence'
                        }
                    },
                    't2': {
                        'header': {
                            'SeriesDescription': 'T2_sequence'
                    }}}}}
        tr_1 = Transaction()
        tr_1.last_message = json.dumps(last_message)
        t_id = t_db.create_transaction(tr_1)

        # remove sequences field
        session = t_db.session
        tr_2 = t_db.get_transaction(t_id)
        tr_2.institution = ''
        session.commit()
        self.assertEqual('', t_db.get_transaction(t_id).institution)

        # execute migrate python script
        model = get_transaction_model(engine)
        migrations.migrate_sequences(session, model)
        session.commit()
        tr_2 = t_db.get_transaction(t_id)
        self.assertEqual('T1_sequence;T2_sequence', tr_2.sequences)

        t_db.close()

    def test_migrate_study_date(self):
        engine = self._get_temp_db(4)
        t_db = TransactionDB(engine)
        last_message = {
            'data': {
                'dicom_info': {
                    't1': {
                        'header': {
                            'StudyDate': '20190101'
                        }
                    }}}}
        tr_1 = Transaction()
        tr_1.last_message = json.dumps(last_message)
        t_id = t_db.create_transaction(tr_1)
        tr_1 = t_db.get_transaction(t_id)
        # by default TransactionsDB doesn't set this field
        self.assertEqual(None, tr_1.study_date)

        # execute migrate python script
        model = get_transaction_model(engine)
        migrations.migrate_study_date(t_db.session, model)
        t_db.session.commit()

        tr_2 = t_db.get_transaction(t_id)
        self.assertEqual('20190101', tr_2.study_date)

        t_db.close()

    def test_migrate_version(self):
        engine = self._get_temp_db(5)
        t_db = TransactionDB(engine)
        last_message = {'data': {'version': '2.2.1'}}
        tr_1 = Transaction()
        tr_1.last_message = json.dumps(last_message)
        t_id = t_db.create_transaction(tr_1)
        tr_1 = t_db.get_transaction(t_id)
        # by default TransactionsDB doesn't set this field
        self.assertEqual(None, tr_1.version)

        # execute migrate python script
        model = get_transaction_model(engine)
        migrations.migrate_version(t_db.session, model)
        t_db.session.commit()

        tr_2 = t_db.get_transaction(t_id)
        self.assertEqual('2.2.1', tr_2.version)
        t_db.close()

    def test_migrate_analysis_type(self):
        engine = self._get_temp_db(6)
        t_db = TransactionDB(engine)
        last_message = {'data': {'report_pdf_paths': {'mdbrain_nd': 'path1'}}}
        tr_1 = Transaction()
        tr_1.last_message = json.dumps(last_message)
        t_id = t_db.create_transaction(tr_1)
        tr_1 = t_db.get_transaction(t_id)
        # by default TransactionsDB doesn't set this field
        self.assertEqual(None, tr_1.analysis_type)

        # execute migrate python script
        model = get_transaction_model(engine)
        migrations.migrate_analysis_types(t_db.session, model)
        t_db.session.commit()

        tr_2 = t_db.get_transaction(t_id)
        self.assertEqual('mdbrain_nd', tr_2.analysis_type)
        t_db.close()

    def test_migrate_analysis_type_2(self):
        engine = self._get_temp_db(7)
        t_db = TransactionDB(engine)
        last_message = {
            'data': {
                'report_pdf_paths':
                    {'mdbrain_nd': 'path1', 'mdbrain_ms': 'path2'}}}
        tr_1 = Transaction()
        tr_1.last_message = json.dumps(last_message)
        t_id = t_db.create_transaction(tr_1)
        tr_1 = t_db.get_transaction(t_id)
        # by default TransactionsDB doesn't set this field
        self.assertEqual(None, tr_1.analysis_type)

        # execute migrate python script
        model = get_transaction_model(engine)
        migrations.migrate_analysis_types(t_db.session, model)
        t_db.session.commit()

        tr_2 = t_db.get_transaction(t_id)
        self.assertTrue('mdbrain_ms' in tr_2.analysis_type)
        self.assertTrue('mdbrain_nd' in tr_2.analysis_type)
        t_db.close()

    def test_migrate_report_qa(self):
        engine = self._get_temp_db(8)
        t_db = TransactionDB(engine)
        last_message = {
            'data': {'report_qa_score_outcomes': {'mdbrain_nd': 'good'}}}
        tr_1 = Transaction()
        tr_1.last_message = json.dumps(last_message)
        t_id = t_db.create_transaction(tr_1)
        tr_1 = t_db.get_transaction(t_id)
        # by default TransactionsDB doesn't set this field
        self.assertEqual(None, tr_1.qa_score)

        # execute migrate python script
        model = get_transaction_model(engine)
        migrations.migrate_qa_scores(t_db.session, model)
        t_db.session.commit()

        tr_2 = t_db.get_transaction(t_id)
        self.assertEqual('good', tr_2.qa_score)
        t_db.close()

    def test_migrate_report_qa_2(self):
        engine = self._get_temp_db(9)
        t_db = TransactionDB(engine)
        last_message = {
            'data': {
                'report_qa_score_outcomes': {
                    'mdbrain_nd': 'good', 'mdbrain_ms': 'acceptable'}}}
        tr_1 = Transaction()
        tr_1.last_message = json.dumps(last_message)
        t_id = t_db.create_transaction(tr_1)
        tr_1 = t_db.get_transaction(t_id)
        # by default TransactionsDB doesn't set this field
        self.assertEqual(None, tr_1.qa_score)

        # execute migrate python script
        model = get_transaction_model(engine)
        migrations.migrate_qa_scores(t_db.session, model)
        t_db.session.commit()

        tr_2 = t_db.get_transaction(t_id)
        self.assertTrue('mdbrain_ms:acceptable' in tr_2.qa_score)
        self.assertTrue('mdbrain_nd:good' in tr_2.qa_score)
        t_db.close()

    @unittest.skip("Feature removed, test kept for future re-introduction")
    def test_migrations_site_id_foreign_key(self):
        "Test that transactions table is migrated with site_id foreign key."""
        temp_folder = tempfile.mkdtemp(suffix='_test_migrations_site_id')
        self.addCleanup(shutil.rmtree, temp_folder)
        temp_db_path = os.path.join(temp_folder, 't_v1.db')
        shutil.copy('tests/fixtures/t_v1.db', temp_db_path)
        engine = create_engine('sqlite:///' + temp_db_path)

        with patch('mediaire_toolbox.constants.TRANSACTIONS_DB_SCHEMA_VERSION',
                   return_value=16):
            # should execute all migrations code
            t_db = TransactionDB(engine,
                                 create_db=True,
                                 db_file_path=temp_db_path)
        t = Transaction()
        t_id = t_db.create_transaction(t)

        # https://stackoverflow.com/a/54029747/894166
        meta = MetaData()
        session = t_db.session
        meta.reflect(bind=engine)
        self.assertIn('sites.id',
                      [e.target_fullname
                       for e in meta.tables['transactions'].foreign_keys])
        new_t = t_db.get_transaction(t_id)
        self.assertEqual(new_t.site_id, 0)

    def test_migrations_site_id_default(self):
        "Test that transactions table is migrated with site_id foreign key."""
        temp_folder = tempfile.mkdtemp(suffix='_test_migrations_site_id')
        self.addCleanup(shutil.rmtree, temp_folder)
        temp_db_path = os.path.join(temp_folder, 't_v1.db')
        shutil.copy('tests/fixtures/t_v1.db', temp_db_path)
        engine = create_engine('sqlite:///' + temp_db_path)
        t_db = TransactionDB(engine, create_db=True, db_file_path=temp_db_path)

        # check if all existing transactions were migrated successfully
        for migrated_t in t_db.session.query(Transaction):
            with self.subTest(t_id=migrated_t.transaction_id):
                self.assertEqual(migrated_t.site_id, 0)

        # check that newly created transactions are inserted w/ correct default
        t_id_new = t_db.create_transaction(Transaction())
        t_new = t_db.get_transaction(t_id_new)
        self.assertEqual(t_new.site_id, 0)

    def test_migrations_users_sites_foreign_keys(self):
        "Test that transactions table is migrated with site_id foreign key."""
        temp_folder = tempfile.mkdtemp(suffix='_test_migrations_site_id')
        self.addCleanup(shutil.rmtree, temp_folder)
        temp_db_path = os.path.join(temp_folder, 't_v1.db')
        shutil.copy('tests/fixtures/t_v1.db', temp_db_path)
        engine = create_engine('sqlite:///' + temp_db_path)

        t_db = TransactionDB(engine,
                             create_db=True,
                             db_file_path=temp_db_path)

        # https://stackoverflow.com/a/54029747/894166
        meta = MetaData()
        session = t_db.session
        meta.reflect(bind=engine)

        self.assertTrue(all(
            foreign_key in [e.target_fullname
                            for e in meta.tables['users_sites'].foreign_keys]
            for foreign_key in ['users.id', 'sites.id']
        ))

    def test_migrations_patient_consent_date(self):
        """Test that patient_consent_date is migrated correctly."""
        engine = self._get_temp_db(10)
        t_db = TransactionDB(engine)
        # TODO is this a problem?
        # TZDateTime apparently does not re-serialize microsceconds correctly
        now = utcnow().replace(microsecond=0)

        t = Transaction()
        t.patient_consent = 0
        t_id_no_consent = t_db.create_transaction(t)

        t = Transaction()
        t.patient_consent = 1
        t.data_uploaded = now
        t_id_date_uploaded = t_db.create_transaction(t)

        # TODO bcause the setter is overwritten in the new version, the legacy
        # field cannot be set and tested
        t = Transaction()
        t.patient_consent = 1
        t.data_uploaded = None
        t_id_no_uploaded = t_db.create_transaction(t)

        model = get_transaction_model(engine)
        migrations.migrate_institution(t_db.session, model)
        t_db.session.commit()

        with self.subTest(patient_consent=0, data_uploaded=None):
            ret_date = \
                t_db.get_transaction(t_id_no_consent).patient_consent_date
            self.assertIsNone(ret_date)

        with self.subTest(patient_consent=1, data_uploaded=now):
            ret_date = \
                t_db.get_transaction(t_id_date_uploaded).patient_consent_date
            self.assertEqual(ret_date.replace(microsecond=0), now)  # TODO

        # TODO bcause the setter is overwritten in the new version, the legacy
        # field cannot be set and tested
        # with self.subTest(patient_consent=1, data_uploaded=None):
        #     ret_date = \
        #         t_db.get_transaction(t_id_no_uploaded).patient_consent_date
        #     self.assertEqual(ret_date,
        #                      datetime.fromtimestamp(0, tz=timezone.utc))


    # Aadvanced ALTER TABLE from SQLite 3.35.0 is only available in
    # SQLAlchemy >= v1.4, so dropping the column was disabled in the migration
    @unittest.expectedFailure
    def test_migrations_patient_consent_removed(self):
        """Test that patient_consent field is not present anymore."""
        temp_folder = \
            tempfile.mkdtemp(suffix='_test_migrations_patient_consent_removed')
        self.addCleanup(shutil.rmtree, temp_folder)
        temp_db_path = os.path.join(temp_folder, 't_v1.db')
        shutil.copy('tests/fixtures/t_v1.db', temp_db_path)
        engine = create_engine('sqlite:///' + temp_db_path)

        t_db = TransactionDB(engine,
                             create_db=True,
                             db_file_path=temp_db_path)

        # https://stackoverflow.com/a/69653223/894166
        columns = \
            t.db.session.query(Transaction).limit(1).statement.columns.keys()
        self.assertNotIn('patient_consent', columns)
