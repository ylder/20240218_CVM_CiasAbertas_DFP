import os
from glob import glob
from importlib import import_module

from pandas import read_csv


class ContractComplianceValidation:
    def __init__(self, data_validate_path):
        self.data_validate_path = data_validate_path

        if not self.identified_contract():
            raise ValueError(f'Contract for the file not identified. File: {self.data_validate_path}.')

        if not self.load_contract_module():
            raise ValueError(f'Error importing module. File: {self.data_validate_path}.')

        if not self.validate():
            raise ValueError(f'Execution canceled, contract not respected. File: {self.data_validate_path}.')

    def identified_contract(self):
        dfp_document = [
            'dfp_cia_aberta_20', '_BPA', '_BPP', '_DFC_MD',
            '_DFC_MI', '_DMPL', '_DRA', '_DRE', '_DVA', '_parecer'
        ]

        for doc in dfp_document:
            if doc in self.data_validate_path:
                return doc
        return False

    def load_contract_module(self):
        contract_files = glob(os.path.join(os.getcwd(), 'src', 'contracts', '*.py'))

        for contract in contract_files:
            try:
                if self.identified_contract() in contract:
                    module_path = contract[len(os.getcwd()) + 1:-3].replace(os.path.sep, '.')
                    return import_module(module_path)
            except Exception as e:
                print(f'Error importing module: {e}')

        return False

    def data(self, to_validate=False):
        temp_df = read_csv(self.data_validate_path, sep=';', dtype=str)
        temp_df = temp_df.astype(self.load_contract_module().data_types)

        if to_validate:
            return temp_df
        
        name_file = len(os.path.join(os.getcwd(), 'data', 'dfp_data_files')) + 1

        return temp_df.assign(file=self.data_validate_path[name_file:])

    def validate(self):
        contract_module = self.load_contract_module()
        schema_expected = set(contract_module.base_model.__annotations__)
        schema_actual = set(self.data(to_validate=True).columns)

        unexpected_columns = schema_actual - schema_expected
        missing_columns = schema_expected - schema_actual

        if unexpected_columns or missing_columns:
            print(
                "Table has different columns than expected.",
                f"\nUnexpected columns: {unexpected_columns}",
                f"\nMissing columns: {missing_columns}"
            )
            return False

        for index, row in self.data(to_validate=True).iterrows():
            row_dict = row.to_dict()
            contract_module.base_model.model_validate(row_dict)

        return True

if __name__ == "__main__":
    data_file_path = r'C:\portfolio-projects\20240218_CVM_CiasAbertas_DFP\data\dfp_cia_aberta_2015.csv'
    ContractComplianceValidation(data_file_path).data()
