from http import HTTPStatus
from io import BytesIO
from os import getcwd, makedirs
from os.path import isfile, join
from pathlib import Path
from tempfile import TemporaryDirectory
from time import sleep
from zipfile import ZipFile

from pandas import DataFrame, read_csv
from requests import get
from requests.exceptions import HTTPError


class Extractor:
    def __init__(self, retry_codes=None, max_failures=3, start_year=2015, end_year=2017, full_extract=False):
        self.retry_codes = retry_codes or [
            HTTPStatus.INTERNAL_SERVER_ERROR,
            HTTPStatus.BAD_GATEWAY,
            HTTPStatus.SERVICE_UNAVAILABLE,
            HTTPStatus.GATEWAY_TIMEOUT,
        ]
        self.max_failures = max_failures
        self.start_year = start_year
        self.end_year = end_year
        self.full_extract = full_extract
        self.extract_dir = join(getcwd(), 'data', 'dfp_data_files')
        self.temp_dir = join(getcwd(), 'temp')
        self.archive = join(getcwd(), 'data', 'extracted_file_history.csv')

        makedirs(self.extract_dir, exist_ok=True)
        makedirs(self.temp_dir, exist_ok=True)

        if not isfile(self.archive):
            DataFrame({'nomes': []}).to_csv(self.archive, index=False)

        if self.full_extract:
            self._reset_history()

    def _reset_history(self):
        DataFrame({'nomes': []}).to_csv(self.archive, index=False)

    def _check_year_extracted(self, year):
        extracted_years = read_csv(self.archive)['nomes'].tolist()
        if year in extracted_years:
            print(f"Year already extracted: {year}")
            return True
        return False

    def _download_with_retry(self, url):
        for attempt in range(self.max_failures):
            try:
                response = get(url)
                response.raise_for_status()
                return response
            except HTTPError as exc:
                if exc.response.status_code in self.retry_codes:
                    sleep(10)
                else:
                    raise exc
        raise HTTPError(f"Failed to download {url} after {self.max_failures} attempts")

    def _save_files(self, path):
        for file in Path(path).glob('*.csv'):
            with open(file, "r", encoding="cp1252") as src_file:
                new_file_path = join(self.extract_dir, file.relative_to(path))
                with open(new_file_path, 'w', encoding='utf-8', newline='\n') as dst_file:
                    dst_file.write(src_file.read())

    def _extract_files(self, response):
        with TemporaryDirectory(dir=self.temp_dir) as temp_path:
            with ZipFile(BytesIO(response.content)) as zip_file:
                zip_file.extractall(temp_path)
                self._save_files(temp_path)

    def extract(self):
        base_url = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/"

        for year in range(self.start_year, self.end_year + 1):
            if self._check_year_extracted(year):
                continue

            zip_url = f"{base_url}dfp_cia_aberta_{year}.zip"
            response = self._download_with_retry(zip_url)
            self._extract_files(response)

            with open(self.archive, 'a', encoding='utf-8') as history_file:
                history_file.write(f"{year}\n")
                print(f"Added to history: {year}")


if __name__ == '__main__':
    extractor = Extractor(retry_codes=None, max_failures=3, start_year=2015, end_year=2017, full_extract=True)
    extractor.extract()
