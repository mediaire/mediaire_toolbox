import os
import shutil
import tempfile

from sqlalchemy import create_engine


class TempDBFactory():
    """Helper to create independent databases for each test"""

    def __init__(self, test_suite_name):
        self.test_suite_name = test_suite_name
        self.test_index = 0
        self.temp_folder = None

    def get_temp_db(self):
        self.temp_folder = \
            tempfile.mkdtemp(suffix='_{}_'.format(self.test_suite_name))
        self.test_index += 1
        return create_engine(
            'sqlite:///{}?check_same_thread=False'
            .format(os.path.join(self.temp_folder,
                                 't{}.db'.format(self.test_index)))
        )

    def delete_temp_folder(self):
        if self.temp_folder:
            shutil.rmtree(self.temp_folder)
            self.temp_folder = None
