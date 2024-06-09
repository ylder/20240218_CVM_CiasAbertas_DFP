from pydantic import BaseModel
from datetime import datetime

class base_model(BaseModel):
    CNPJ_CIA: str
    DENOM_CIA: str
    DT_REFER: datetime
    NUM_ITEM_PARECER_DECL: int
    TP_PARECER_DECL: str
    TP_RELAT_AUD: str
    TXT_PARECER_DECL: str
    VERSAO: int

data_types = {
    'CNPJ_CIA': str,
    'DENOM_CIA': str,
    'DT_REFER': 'datetime64[ns]',
    'NUM_ITEM_PARECER_DECL': int,
    'TP_PARECER_DECL': str,
    'TP_RELAT_AUD': str,
    'TXT_PARECER_DECL': str,
    'VERSAO': int
}