from http import HTTPStatus
from io import BytesIO
from os import getcwd, makedirs
from os.path import join
from pathlib import Path
from tempfile import TemporaryDirectory
from time import sleep
from zipfile import ZipFile

from pydantic import StrictInt
from requests import get
from requests.exceptions import HTTPError


class Extractor():

    def __init__(
        self,
        retry_codes: list,
        max_failures: StrictInt,
        start_year: StrictInt,
        end_year: StrictInt,
    ):
        self.retry_codes = retry_codes
        self.max_failures = max_failures
        self.start_year = start_year
        self.end_year = end_year
        self.extract_dir = join(getcwd(), 'data', 'dfp_data_files')
        self.temp_dir = join(getcwd(), 'temp')

        makedirs(self.extract_dir, exist_ok=True)
        makedirs(self.temp_dir, exist_ok=True)

    def _download_path_with_retry(self, url):
        if self.retry_codes is None:
            retry_codes = [
                HTTPStatus.INTERNAL_SERVER_ERROR,
                HTTPStatus.BAD_GATEWAY,
                HTTPStatus.SERVICE_UNAVAILABLE,
                HTTPStatus.GATEWAY_TIMEOUT,
            ]

        for retry in range(3):
            try:
                response = get(url)
                response.raise_for_status()
                return response

            except HTTPError as exc:
                code = exc.response.status_code
                if code in retry_codes:
                    sleep(10)
                    continue
                raise exc
    
    def _files_deconding_and_saving(self, path):
        for file in Path(path).glob('*.csv'):
            with open(file, "r", encoding="cp1252") as copied_file:                   
                new_file = join(self.extract_dir, file.relative_to(path))
                with open(new_file, 'w', encoding='utf-8', newline='\n') as file_copy:
                    file_copy.write(copied_file.read())
    
    def _extract_dfp_files(self, response):
        with TemporaryDirectory(dir=self.temp_dir) as temp_path:
            with ZipFile(BytesIO(response.content)) as zip_file:
                zip_file.extractall(temp_path)
                self._files_deconding_and_saving(temp_path)

    def extract(self):
        base_url = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/"

        for year in range(self.start_year, self.end_year + 1):
            zip_path_url = base_url + f"dfp_cia_aberta_{year}.zip"
            response = self._download_path_with_retry(zip_path_url)
            self._extract_dfp_files(response)


if __name__ == '__main__':
    inst = Extractor(retry_codes=None, max_failures=3, start_year=2015, end_year=2015)
    inst.extract()