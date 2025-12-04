import json
import json
import datetime
import re
from typing import List, Dict, Any
import pandas as pd
from ..utils.db import execute, fetch, fetch_one_off, executemany_one_off
import logging

logger = logging.getLogger(__name__)


def _parse_number(val: Any) -> Any:
    """Парсит числовые строки в float/int. Возвращает оригинал, если парсинг не удался."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return val
    s = str(val).strip()
    if s == '':
        return None
    
    # Basic cleanup
    s = s.replace('\xa0', '').replace(' ', '')
    
    # Handle parentheses for negative numbers
    neg = False
    if s.startswith('(') and s.endswith(')'):
        neg = True
        s = s[1:-1]
        
    # Remove currency symbols
    s = s.replace('$', '').replace('€', '').replace('₽', '')
    
    # Heuristics for separators
    # If we have both comma and dot, assume the last one is the decimal separator
    if ',' in s and '.' in s:
        if s.rfind('.') > s.rfind(','):
            s = s.replace(',', '') # comma is thousand sep
        else:
            s = s.replace('.', '') # dot is thousand sep
            s = s.replace(',', '.') # comma is decimal sep
    elif ',' in s:
        # If only comma, check if it looks like a decimal or thousand separator
        # This is tricky. "1,234" vs "1,23" (could be 1.23).
        # Default assumption: if 3 digits after comma, it MIGHT be thousands, but 
        # in many European formats "1,234" is 1.234. 
        # Given the context of "data from sheets", we often get what the locale sends.
        # Let's try to be safe: if it parses as float with comma replaced by dot, use that.
        # BUT, "1,000" -> 1.0 is wrong if it meant 1000.
        # Let's assume standard US/UK if no other info, UNLESS we see it's likely European.
        # Actually, let's just try to replace comma with dot if it makes sense.
        # A safer bet for "A-AF" columns which likely contain mixed data:
        # If the string matches a simple float pattern with comma, replace it.
        parts = s.split(',')
        if len(parts) == 2 and len(parts[1]) != 3:
             s = s.replace(',', '.')
        else:
             s = s.replace(',', '') # Assume it was a thousand separator or just noise if it fails later
             
    # Remove non-numeric chars (except dot and minus)
    s = re.sub(r"[^0-9.\-]", '', s)
    
    try:
        if '.' in s:
            v = float(s)
        else:
            v = int(s)
        return -v if neg else v
    except Exception:
        return val


def _parse_date(val: Any) -> Any:
    if val is None:
        return None
    if isinstance(val, datetime.datetime):
        return val.astimezone(datetime.timezone.utc)
    s = str(val).strip()
    if s == '':
        return None
    try:
        # Try ISO format first
        d = datetime.datetime.fromisoformat(s)
        if d.tzinfo is None:
            d = d.replace(tzinfo=datetime.timezone.utc)
        return d.astimezone(datetime.timezone.utc)
    except ValueError:
        pass
        
    # Try common formats
    formats = [
        '%d.%m.%Y', '%d.%m.%Y %H:%M', '%Y-%m-%d', '%Y-%m-%d %H:%M:%S',
        '%m/%d/%Y', '%m/%d/%Y %H:%M'
    ]
    
    for fmt in formats:
        try:
            d = datetime.datetime.strptime(s, fmt)
            return d.replace(tzinfo=datetime.timezone.utc)
        except ValueError:
            continue
            
    return None


def _normalize_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Возвращает новый словарь payload с распаршенными датами/числами/валютами, где это возможно."""
    out = {}
    # We want to preserve the order if possible, but dicts are ordered in modern Python.
    # We will iterate through all keys.
    for k, v in (payload or {}).items():
        key = k.strip() if isinstance(k, str) else k
        # clean key name? maybe not, let's keep it as is for now but maybe strip whitespace
        
        # Attempt to parse values based on content, since column names might be generic
        # or in Russian/English mixed.
        
        if v is None or v == "":
            out[key] = None
            continue
            
        # Try to parse as number first
        parsed_num = _parse_number(v)
        if isinstance(parsed_num, (int, float)):
            out[key] = parsed_num
            continue
            
        # Try to parse as date
        parsed_date = _parse_date(v)
        if parsed_date is not None:
            out[key] = parsed_date.isoformat() # Store as ISO string in JSON
            continue
            
        # Fallback: string cleanup
        if isinstance(v, str):
            out[key] = v.strip()
        else:
            out[key] = v
            
    return out


async def normalize_to_staging(source: str, records: List[Dict[str, Any]]):
    """Нормализует сырые записи в staging.data.

    records может быть списком словарей (payloads) или строк, извлеченных из raw.data,
    где каждая строка имеет поля 'id' и 'payload'.
    """
    if not records:
        return pd.DataFrame()

    rows = []
    for r in records:
        rec_id = None
        payload = None
        if isinstance(r, dict) and 'payload' in r and 'id' in r:
            rec_id = r.get('id')
            payload = r.get('payload')
        else:
            payload = r
        # If payload was stored as a JSON string in the DB, deserialize it so
        # normalization can operate on a dict. If it isn't JSON, leave as-is.
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except Exception:
                # leave payload as string if it isn't valid JSON
                pass
        rows.append({
            'id': rec_id or (payload.get('id') if isinstance(payload, dict) else None) or (payload.get('source_id') if isinstance(payload, dict) else None),
            'source': source,
            'created_at': datetime.datetime.now(datetime.timezone.utc),
            'payload': _normalize_payload(payload) if isinstance(payload, dict) else payload,
        })

    # Avoid pandas here to preserve native dict types for payload.
    # Batch the inserts and use a one-off executemany to avoid creating a persistent pool
    sql = "INSERT INTO staging.data (id, source, created_at, payload, payment_ts, type_text, total_rub_num) VALUES ($1,$2,$3,$4::jsonb,$5,$6,$7) ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload, payment_ts = EXCLUDED.payment_ts, type_text = EXCLUDED.type_text, total_rub_num = EXCLUDED.total_rub_num"
    def args_iter():
        for row in rows:
            try:
                payload = row['payload']
                if not isinstance(payload, str):
                    # use default=str to serialize datetime objects to ISO strings
                    payload = json.dumps(payload, ensure_ascii=False, default=str)
                
                # extract typed fields from normalized payload (payload may be dict before serialization)
                norm = row['payload'] if isinstance(row['payload'], dict) else None
                payment_ts = None
                type_text = None
                total_rub_num = None
                
                if isinstance(norm, dict):
                    # Try to find payment date in likely columns
                    pd_val = norm.get('Payment date') or norm.get('Date') or norm.get('Дата')
                    if pd_val:
                         # if pd is a datetime, keep it; if string, try ISO parse
                        if hasattr(pd_val, 'tzinfo'):
                            payment_ts = pd_val
                        else:
                            try:
                                import datetime as _dt
                                payment_ts = _dt.datetime.fromisoformat(str(pd_val))
                                if payment_ts.tzinfo is None:
                                    payment_ts = payment_ts.replace(tzinfo=_dt.timezone.utc)
                            except Exception:
                                payment_ts = None
                                
                    type_text = norm.get('Type') or norm.get('Тип')
                    
                    # total_rub_num: prefer already parsed numeric values
                    tr = norm.get('Total RUB') or norm.get('РУБ сумма') or norm.get('Total in currency') or norm.get('Amount') or norm.get('Сумма')
                    if isinstance(tr, (int, float)):
                        total_rub_num = tr
                    else:
                        try:
                            total_rub_num = float(str(tr)) if tr is not None and str(tr) != '' else None
                        except Exception:
                            total_rub_num = None

                yield (row['id'], row['source'], row['created_at'], payload, payment_ts, type_text, total_rub_num)
            except Exception as exc:
                logger.warning('⚠️ Не удалось подготовить строку staging: %s', exc)

    # perform bulk insert using one-off executemany to avoid holding pool connections
    try:
        await executemany_one_off(sql, args_iter())
    except Exception as exc:
        logger.warning('⚠️ Не удалось выполнить executemany для строк staging: %s', exc)
    return rows


async def transform_from_raw(source: str, limit: int = 1000):
    """Извлекает строки из raw.data для заданного источника и нормализует их в staging."""
    sql = "SELECT id, payload FROM raw.data WHERE source = $1 ORDER BY extracted_at DESC LIMIT $2"
    # use one-off fetch to avoid creating a persistent pool for this short-lived script
    rows = await fetch_one_off(sql, source, limit)
    records = []
    for r in rows:
        records.append({'id': r['id'], 'payload': r['payload']})
    return await normalize_to_staging(source, records)
