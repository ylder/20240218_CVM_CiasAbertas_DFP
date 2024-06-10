from src.modules.extract import Extractor
from src.modules.ingestion import Ingestion
from glob import glob
from os import getcwd
from os.path import join

# Extractor(retry_codes=None, max_failures=3, start_year=2015, end_year=2023).extract()

arquivos = glob(join(getcwd(), 'data', 'dfp_data_files', '*.csv'))
for i in arquivos:
    Ingestion('dfp_cias_abertas.db', i).run()