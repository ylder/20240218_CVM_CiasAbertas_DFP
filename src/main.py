from glob import glob
from os import getcwd
from os.path import join

from src.modules.extract import Extractor
from src.modules.ingestion import Ingestion

# Type of load
full_load = True

# Extract data files
extractor = Extractor(retry_codes=None, max_failures=3, start_year=2015, end_year=2023, full_extract=full_load)
extractor.extract()

# Ingest data files into database tables
data_files = glob(join(getcwd(), 'data', 'dfp_data_files', '*.csv'))
ingestion_instance = Ingestion('dfp_cias_abertas.db', full_load=full_load)

for file_path in data_files:
    ingestion_instance.insert_data(file_path)

ingestion_instance._close_connection()
