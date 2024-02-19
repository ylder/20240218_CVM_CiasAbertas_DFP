from io import BytesIO
from time import sleep
from zipfile import ZipFile
from pydantic import BaseModel, ValidationError
from requests import get


class DownloadParameters(BaseModel):
    start_year: int
    end_year: int
    extract_dir: str = "./data/"

    def validate(self):
        if self.start_year > self.end_year:
            raise ValueError("start_year must be less than or equal to end_year")


def download_and_extract_dfp_files(config: DownloadParameters):
    """Downloads and extracts DFP files from the CVM's open data portal."""

    config.validate()

    base_url = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/"
    max_failures = 3

    for year in range(config.start_year, config.end_year + 1):
        file_url = base_url + f"dfp_cia_aberta_{year}.zip"
        successful_download = False

        for attempt in range(1, max_failures + 1):
            try:
                with get(file_url, stream=True) as response:
                    response.raise_for_status()

                    with BytesIO(response.content) as zip_bytes:
                        with ZipFile(zip_bytes) as zip_file:
                            zip_file.extractall(config.extract_dir)

                    print(f"File {year} downloaded and extracted successfully.")
                    successful_download = True
                    break

            except Exception as e:  # Catch broader range of exceptions
                print(f"Failed to download/extract file {year} (attempt {attempt}/{max_failures}): {e}")

                if attempt < max_failures:
                    print(f"Waiting for {30} seconds before retrying...")
                    sleep(30)

        if not successful_download:
            raise RuntimeError(f"Download failed for {file_url} after {max_failures} attempts")

    print(f"Collection completed ({config.start_year}-{config.end_year}).")

# Example usage:
try:
    config = DownloadParameters(start_year=2010, end_year=2023)
    download_and_extract_dfp_files(config)
except ValidationError as e:
    print("Invalid configuration:", e)

