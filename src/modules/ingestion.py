from os import getcwd
from os.path import isfile, join

from duckdb import CatalogException, connect
from pandas import DataFrame, read_csv

from src.modules.data_quality import ContractComplianceValidation


class Ingestion:
    def __init__(self, database, data_insert_path, full_load=False):
        self.conn = connect(database)
        self.data_insert_path = data_insert_path
        self.archive = join(getcwd(), 'data', 'ingested_file_history.csv')
        self.table = self.table_name()
        self.full_load = full_load
        self.file_already_ingested = False

        if not isfile(self.archive):
            DataFrame({'nomes': ()}).to_csv(self.archive, index=False)

        if self._check_file_ingested():
            self.file_already_ingested = True

        if self.full_load:
            self._delete_files_from_history()
            self._delete_data_from_table()


    def _check_file_ingested(self):
        names = read_csv(self.archive)['nomes'].to_list()

        if self.data_insert_path in names:
            print(f"File already ingested. File: {self.data_insert_path}")
            return True

        return False

    def _delete_files_from_history(self):
        DataFrame({'nomes': ()}).to_csv(self.archive, index=False)

    def table_name(self):
        dfp_document = [
            'dfp_cia_aberta_20', '_BPA', '_BPP', '_DFC_MD',
            '_DFC_MI', '_DMPL', '_DRA', '_DRE', '_DVA', '_parecer'
        ]

        for doc in dfp_document:
            if doc in self.data_insert_path:
                if doc.startswith('_', 0):
                    return doc[1:].upper()
                return doc[:-3].upper()

    def table_exists(self):
        try:
            self.conn.execute(f"SELECT 1 FROM {self.table} LIMIT 1")
            return True
        except CatalogException:
            return False

    def _delete_data_from_table(self):
        if self.table_exists():
            self.conn.execute(f'DELETE FROM {self.table}')

    def _insert_data(self):
        if self.file_already_ingested:
            return

        data = ContractComplianceValidation(self.data_insert_path).data()
        data = self.conn.from_df(data)

        if not self.table_exists():
            data.create(self.table)
        else:
            data.insert_into(self.table)

        print(f'File inserted: {self.data_insert_path}.')

        with open(self.archive, 'a', encoding='utf-8') as file_history:
            file_history.write(self.data_insert_path + '\n')
            print("Added to history.")

    def _close_connection(self):
        self.conn.close()

    def run(self):
        self._insert_data()
        self._close_connection()

if __name__ == "__main__":

    dir = r'C:\portfolio-projects\20240218_CVM_CiasAbertas_DFP\data\dfp_data_files\dfp_cia_aberta_BPA_con_2015.csv'
    Ingestion('teste.db', dir).run()