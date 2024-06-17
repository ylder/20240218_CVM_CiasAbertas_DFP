import os
from glob import glob
from importlib import import_module

import pandas as pd


class ContractComplianceValidation:
    def __init__(self, data_path):
        self.data_path = data_path
        self.contract = self._identify_contract()
        self.contract_module = self._load_contract_module()

        if not self.contract:
            raise ValueError(f'Contract not identified for file: {self.data_path}.')

        if not self.contract_module:
            raise ValueError(f'Error importing module for file: {self.data_path}.')

        if not self._validate_data():
            raise ValueError(f'Validation failed for file: {self.data_path}.')

    def _identify_contract(self):
        dfp_documents = [
            'dfp_cia_aberta_20', '_BPA', '_BPP', '_DFC_MD',
            '_DFC_MI', '_DMPL', '_DRA', '_DRE', '_DVA', '_parecer'
        ]

        for doc in dfp_documents:
            if doc in self.data_path:
                return doc
        return None

    def _load_contract_module(self):
        contract_files = glob(os.path.join(os.getcwd(), 'src', 'contracts', '*.py'))

        for contract in contract_files:
            if self.contract in contract:
                module_path = contract[len(os.getcwd()) + 1:-3].replace(os.path.sep, '.')
                try:
                    return import_module(module_path)
                except Exception as e:
                    print(f'Error importing module: {e}')
                    return None

        return None

    def _load_data(self, to_validate=False):
        df = pd.read_csv(self.data_path, sep=';', dtype=str)
        df = df.astype(self.contract_module.data_types)

        if to_validate:
            return df

        file_col_name = os.path.basename(self.data_path)
        return df.assign(file=file_col_name)

    def _validate_data(self):
        expected_schema = set(self.contract_module.base_model.__annotations__)
        actual_schema = set(self._load_data(to_validate=True).columns)

        unexpected_columns = actual_schema - expected_schema
        missing_columns = expected_schema - actual_schema

        if unexpected_columns or missing_columns:
            print(
                "Schema mismatch detected.",
                f"Unexpected columns: {unexpected_columns}",
                f"Missing columns: {missing_columns}"
            )
            return False

        for _, row in self._load_data(to_validate=True).iterrows():
            self.contract_module.base_model.model_validate(row.to_dict())

        return True

    def get_validated_data(self):
        return self._load_data()


if __name__ == "__main__":
    data_file_path = r'C:\portfolio-projects\20240218_CVM_CiasAbertas_DFP\data\dfp_cia_aberta_2015.csv'
    validator = ContractComplianceValidation(data_file_path)
    validated_data = validator.get_validated_data()
    print(validated_data.head())
