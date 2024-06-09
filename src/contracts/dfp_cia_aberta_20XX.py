from pydantic import BaseModel
from datetime import datetime

class base_model(BaseModel):
    CATEG_DOC: str
    CD_CVM: str
    CNPJ_CIA: str
    DENOM_CIA: str
    DT_RECEB: datetime
    DT_REFER: datetime
    ID_DOC: int
    LINK_DOC: str
    VERSAO: int

data_types = {
    'CATEG_DOC': str,
    'CD_CVM': str,
    'CNPJ_CIA': str,
    'DENOM_CIA': str,
    'DT_RECEB': 'datetime64[ns]',
    'DT_REFER': 'datetime64[ns]',
    'ID_DOC': int,
    'LINK_DOC': str,
    'VERSAO': int
}