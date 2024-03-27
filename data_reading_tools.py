import re
import pandas as pd


def remove_arabic(text):
    if type(text) is pd.Timestamp:
        return text
    text = re.sub(r'[أ-ي]', '', text)
    text = text.replace('г. ', '')
    text = text.replace(' 上午', '')
    text = text.replace(' μ.μ.', ' PM')
    return text


def convert_datetime(x):
    try:
        x = pd.to_datetime(x, format='mixed')
    except:
        try:
            x = pd.to_datetime(x, format='%d.%m.%Y. %H.%M.%S')
        except:
            try:
                x = pd.to_datetime(x, format='%d/%m/%Y %H.%M.%S')
            except Exception as e:
                raise e
    return x


def fix_datetime(data: pd.DataFrame) -> pd.DataFrame:
    data['sdk_date'] = data['sdk_date'].apply(remove_arabic)
    data['sdk_date'] = data['sdk_date'].apply(convert_datetime)
    return data
