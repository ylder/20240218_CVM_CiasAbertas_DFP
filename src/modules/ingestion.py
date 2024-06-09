import duckdb

class Ingestion:
    def __init__(self, database, table, data_insert_path, full_load=False):
        self.conn = duckdb.connect(database)
        self.table = table
        self.data_insert_path = data_insert_path
        self.full_load = full_load
        
        if self.full_load:
            self.delete_data_from_table()

    def table_exists(self):
        try:
            self.conn.execute(f"SELECT 1 FROM {self.table} LIMIT 1")
            return True
        except duckdb.CatalogException:
            return False

    def delete_data_from_table(self):
        if self.table_exists():
            self.conn.execute(f'DELETE FROM {self.table}')

    def insert_data(self):
        data = duckdb.read_csv(self.data_insert_path, delimiter=';', connection=self.conn)
        if not self.table_exists():
            data.create(self.table)
        else:
            data.insert_into(self.table)

    def close_connection(self):
        self.conn.close()

    def run(self):
        self.insert_data()
        self.close_connection()

if __name__ == "__main__":

    dir = r'C:\portfolio-projects\20240218_CVM_CiasAbertas_DFP\data\dfp_cia_aberta_BPA_con_2015.csv'
    Ingestion('teste.db', 'teste', dir).run()