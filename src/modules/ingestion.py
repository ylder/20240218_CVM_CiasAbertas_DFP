from os import getcwd
from os.path import isfile, join

import pandas as pd
from duckdb import CatalogException, connect

from src.modules.data_quality import ContractComplianceValidation


class Ingestion:
    def __init__(self, database, full_load=False):
        self.conn = connect(database)
        self.archive = join(getcwd(), 'data', 'ingested_file_history.csv')
        self.full_load = full_load

        if not isfile(self.archive):
            pd.DataFrame({'nomes': []}).to_csv(self.archive, index=False)

        if self.full_load:
            self._reset_history()
            self._reset_database()

    def _reset_history(self):
        pd.DataFrame({'nomes': []}).to_csv(self.archive, index=False)

    def _reset_database(self):
        tables = self.conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()

        for table in tables:
            table_name = table[0]
            drop_query = f"DROP TABLE IF EXISTS {table_name}"
            self.conn.execute(drop_query)
            print(f"Table {table_name} deleted.")

        print("All tables have been excluded.")

    def _check_file_ingested(self, file_path):
        ingested_files = pd.read_csv(self.archive)['nomes'].tolist()
        if file_path in ingested_files:
            print(f"File already ingested: {file_path}")
            return True
        return False

    def _get_table_name(self, file_path):
        dfp_documents = [
            'dfp_cia_aberta_20', '_BPA', '_BPP', '_DFC_MD',
            '_DFC_MI', '_DMPL', '_DRA', '_DRE', '_DVA', '_parecer'
        ]

        for doc in dfp_documents:
            if doc in file_path:
                if doc.startswith('_', 0):
                    return doc[1:].upper()
                return doc[:-3].upper()

    def _table_exists(self, table_name):
        try:
            self.conn.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
            return True
        except CatalogException:
            return False

    def insert_data(self, file_path):
        if self._check_file_ingested(file_path):
            return

        validated = ContractComplianceValidation(file_path)
        data = self.conn.from_df(validated.get_validated_data())

        table_name = self._get_table_name(file_path)
        if not table_name:
            raise ValueError(f"Unable to determine table name for file: {file_path}")

        if not self._table_exists(table_name):
            data.create(table_name)
        else:
            data.insert_into(table_name)

        print(f"File inserted: {file_path}")

        with open(self.archive, 'a', encoding='utf-8') as history_file:
            history_file.write(f"{file_path}\n")
            print("Added to history.")

    def _close_connection(self):
        self.conn.close()

if __name__ == "__main__":
    file_path = r'C:\portfolio-projects\20240218_CVM_CiasAbertas_DFP\data\dfp_data_files\dfp_cia_aberta_BPA_con_2015.csv'
    ingestion_instance = Ingestion('teste.db', full_load=True)
    ingestion_instance.insert_data(file_path)
    ingestion_instance._close_connection()
