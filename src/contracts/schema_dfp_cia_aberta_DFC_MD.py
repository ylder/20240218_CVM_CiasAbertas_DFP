from pydantic import BaseModel
from datetime import datetime


class base_model(BaseModel):
    CD_CONTA: str
    CD_CVM: str
    CNPJ_CIA: str
    DENOM_CIA: str
    DS_CONTA: str
    DT_FIM_EXERC: datetime
    DT_INI_EXERC: datetime
    DT_REFER: datetime
    ESCALA_MOEDA: str
    GRUPO_DFP: str
    MOEDA: str
    ORDEM_EXERC: str
    ST_CONTA_FIXA: str
    VERSAO: int
    VL_CONTA: float

data_types = {
    'CD_CONTA': str,
    'CD_CVM': str,
    'CNPJ_CIA': str,
    'DENOM_CIA': str,
    'DS_CONTA': str,
    'DT_FIM_EXERC': 'datetime64[ns]',
    'DT_INI_EXERC': 'datetime64[ns]',
    'DT_REFER': 'datetime64[ns]',
    'ESCALA_MOEDA': str,
    'GRUPO_DFP': str,
    'MOEDA': str,
    'ORDEM_EXERC': str,
    'ST_CONTA_FIXA': str,
    'VERSAO': int,
    'VL_CONTA': float
}
