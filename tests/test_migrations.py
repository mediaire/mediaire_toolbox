import unittest
from unittest.mock import patch
import tempfile
import shutil
import json
import os

from sqlalchemy import create_engine
from sqlalchemy.schema import MetaData

from mediaire_toolbox.transaction_db import migrations
from mediaire_toolbox.transaction_db.transaction_db import (
    TransactionDB,
    get_transaction_model,
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
        engine = self._get_temp_db(5)
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
        engine = self._get_temp_db(5)
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
        engine = self._get_temp_db(5)
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
        engine = self._get_temp_db(5)
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
