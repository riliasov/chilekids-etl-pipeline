"""Transformation utilities for normalizing payload to staging.records."""
import json
import logging
from typing import Any, Dict, Optional, List
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from dateutil import parser as dateutil_parser

from ..utils.db import fetch_one_off
from ..utils.hash import payload_hash

logger = logging.getLogger(__name__)


def _to_timestamptz(val: Any) -> Optional[datetime]:
    """
    Parse value to timezone-aware datetime in UTC.
    
    Supports ISO 8601 and common date formats.
    Returns None if parsing fails.
    """
    if val is None or val == '':
        return None
    if isinstance(val, datetime):
        # Ensure UTC
        if val.tzinfo is None:
            return val.replace(tzinfo=timezone.utc)
        return val.astimezone(timezone.utc)
    
    try:
        dt = dateutil_parser.isoparse(str(val))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (ValueError, TypeError):
        pass
    
    # Try additional common formats
    for fmt in ['%d.%m.%Y', '%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y']:
        try:
            dt = datetime.strptime(str(val), fmt)
            return dt.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            continue
    
    logger.debug(f"Could not parse date: {val}")
    return None


def _to_decimal(val: Any) -> Optional[Decimal]:
    """
    Parse value to Decimal.
    
    Handles:
    - Currency symbols ($, €, ₽)
    - Thousand separators (spaces, commas)
    - Decimal separators (comma, dot)
    - Negative numbers in parentheses: (100) -> -100
    """
    if val is None or val == '':
        return None
    if isinstance(val, Decimal):
        return val
    if isinstance(val, (int, float)):
        return Decimal(str(val))
    
    s = str(val).strip()
    if s == '':
        return None
    
    # Handle negative in parentheses
    neg = False
    if s.startswith('(') and s.endswith(')'):
        neg = True
        s = s[1:-1].strip()
    
    # Remove currency symbols and spaces
    s = s.replace('$', '').replace('€', '').replace('₽', '')
    s = s.replace('\xa0', '').replace(' ', '')
    
    # Determine decimal separator
    # If both comma and dot exist, the last one is likely decimal
    if ',' in s and '.' in s:
        if s.rfind('.') > s.rfind(','):
            s = s.replace(',', '')  # comma is thousands
        else:
            s = s.replace('.', '')  # dot is thousands
            s = s.replace(',', '.')  # comma is decimal
    elif ',' in s:
        # Only comma: check if it's likely decimal or thousands
        # "1,234.56" would have been caught above
        # "1,23" likely decimal, "1,234" ambiguous
        parts = s.split(',')
        if len(parts) == 2 and len(parts[1]) <= 2:
            # Likely decimal
            s = s.replace(',', '.')
        elif len(parts) == 2 and len(parts[1]) == 3:
            # Could be thousands or decimal
            # Default to decimal (European style)
            s = s.replace(',', '.')
        else:
            # Multiple commas or other pattern - treat as thousands
            s = s.replace(',', '')
    
    try:
        result = Decimal(s)
        return -result if neg else result
    except (InvalidOperation, ValueError):
        logger.debug(f"Could not parse decimal: {val}")
        return None


def _to_int(val: Any) -> Optional[int]:
    """Parse value to integer. Returns None if parsing fails."""
    if val is None or val == '':
        return None
    if isinstance(val, int):
        return val
    if isinstance(val, float):
        return int(val)
    
    try:
        # Try to parse as decimal first (handles separators)
        dec = _to_decimal(val)
        if dec is not None:
            return int(dec)
    except Exception:
        pass
    
    try:
        return int(str(val).strip())
    except (ValueError, TypeError):
        logger.debug(f"Could not parse int: {val}")
        return None


def _get(payload: Dict[str, Any], key_variants: List[str]) -> Any:
    """
    Get value from payload by checking multiple key variants.
    
    Checks exact match first, then lowercase without spaces as fallback.
    
    Args:
        payload: Dictionary to search
        key_variants: List of possible key names to try
        
    Returns:
        Value if found, None otherwise
    """
    # Try exact matches first
    for key in key_variants:
        if key in payload:
            return payload[key]
    
    # Try case-insensitive, space-insensitive matching
    normalized_payload = {
        k.lower().replace(' ', ''): v 
        for k, v in payload.items()
    }
    
    for key in key_variants:
        normalized_key = key.lower().replace(' ', '')
        if normalized_key in normalized_payload:
            return normalized_payload[normalized_key]
    
    return None


def normalize_record(
    raw_id: int,
    sheet_row_number: int,
    received_at: datetime,
    payload: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Normalize a raw payload into staging.records format.
    
    Args:
        raw_id: ID from raw.source_events
        sheet_row_number: Row number in the source sheet
        received_at: Timestamp when data was received
        payload: Raw JSONB payload from source
        
    Returns:
        Dictionary with keys matching staging.records columns
    """
    # Compute hash for change detection
    hash_value = payload_hash(payload)
    
    # Extract and normalize all fields
    record = {
        'raw_id': raw_id,
        'sheet_row_number': sheet_row_number,
        'received_at': received_at,
        
        # Date fields
        'date': _to_timestamptz(_get(payload, ['Date', 'Дата', 'date'])),
        'payment_date': _to_timestamptz(_get(payload, [
            'Payment date', 'Payment Date', 'Дата платежа', 'payment_date'
        ])),
        'payment_date_orig': _to_timestamptz(_get(payload, [
            'Payment date (orig)', 'Дата платежа (ориг)', 'payment_date_orig'
        ])),
        
        # Text fields
        'task': _get(payload, ['Task', 'Задача', 'task']),
        'type': _get(payload, ['Type', 'Тип', 'type']),
        'client': _get(payload, ['Client', 'Клиент', 'client']),
        'vendor': _get(payload, ['Vendor', 'Поставщик', 'vendor']),
        'cashier': _get(payload, ['Cashier', 'Кассир', 'cashier']),
        'service': _get(payload, ['Service', 'Услуга', 'service']),
        'approver': _get(payload, ['Approver', 'Утверждающий', 'approver']),
        'category': _get(payload, ['Category', 'Категория', 'category']),
        'currency': _get(payload, ['Currency', 'Валюта', 'currency']),
        'subcategory': _get(payload, ['Subcategory', 'Подкатегория', 'subcategory']),
        'description': _get(payload, ['Description', 'Описание', 'description']),
        'direct_indirect': _get(payload, [
            'Direct/Indirect', 'Прямые/Косвенные', 'direct_indirect'
        ]),
        
        # Category fields (Russian naming)
        'cat_new': _get(payload, ['cat_new', 'Категория новая']),
        'cat_final': _get(payload, ['cat_final', 'Категория финал']),
        'subcat_new': _get(payload, ['subcat_new', 'Подкатегория новая']),
        'subcat_final': _get(payload, ['subcat_final', 'Подкатегория финал']),
        'kategoriya': _get(payload, ['kategoriya', 'Категория']),
        'podstatya': _get(payload, ['podstatya', 'Подстатья']),
        'statya': _get(payload, ['statya', 'Статья']),
        'vidy_raskhodov': _get(payload, ['vidy_raskhodov', 'Виды расходов']),
        
        # Package fields
        'paket': _get(payload, ['paket', 'Пакет', 'package']),
        'package_secondary': _get(payload, [
            'package_secondary', 'package secondary', 'Пакет вторичный'
        ]),
        
        # Numeric fields
        'year': _to_int(_get(payload, ['Year', 'Год', 'year'])),
        'month': _to_int(_get(payload, ['Month', 'Месяц', 'month'])),
        'quarter': _to_int(_get(payload, ['Quarter', 'Квартал', 'quarter'])),
        'count_vendor': _to_int(_get(payload, [
            'Count vendor', 'Количество поставщиков', 'count_vendor'
        ])),
        
        # Decimal fields
        'hours': _to_decimal(_get(payload, ['Hours', 'Часы', 'hours'])),
        'fx_rub': _to_decimal(_get(payload, ['FX RUB', 'Курс РУБ', 'fx_rub'])),
        'fx_usd': _to_decimal(_get(payload, ['FX USD', 'Курс USD', 'fx_usd'])),
        'total_rub': _to_decimal(_get(payload, [
            'Total RUB', 'РУБ сумма', 'total_rub', 'rub_summa', 'РУБ Сумма'
        ])),
        'total_usd': _to_decimal(_get(payload, [
            'Total USD', 'USD сумма', 'total_usd', 'usd_summa'
        ])),
        'sum_total_rub': _to_decimal(_get(payload, [
            'sum Total RUB', 'Сумма РУБ', 'sum_total_rub'
        ])),
        'total_in_currency': _to_decimal(_get(payload, [
            'Total in currency', 'Сумма в валюте', 'total_in_currency'
        ])),
        'rub_summa': _to_decimal(_get(payload, ['rub_summa', 'РУБ Сумма'])),
        'usd_summa': _to_decimal(_get(payload, ['usd_summa', 'USD Сумма'])),
        
        # Metadata
        'payload_hash': hash_value,
        'raw_payload': payload
    }
    
    return record


async def get_changed_raw_records(
    limit: Optional[int] = None,
    batch_size: int = 500
) -> List[Dict[str, Any]]:
    """
    Запрос записей из raw.data для обработки.
    
    Извлекает записи из raw.data, парсит JSON payload и вычисляет хеш.
    
    Args:
        limit: Максимальное количество записей (None для всех)
        batch_size: Размер батча (не используется в запросе, для вызывающего)
        
    Returns:
        Список dict с ключами: raw_id, sheet_row_number, received_at, 
        raw_payload, payload_hash
    """
    # Query raw.data (работает с текущей схемой)
    # Query raw.data optimized with hash check against staging
    sql = """
        SELECT 
            r.id as raw_id,
            r.extracted_at as received_at,
            r.payload,
            r.payload_hash
        FROM raw.data r
        LEFT JOIN staging.records s ON r.payload_hash = s.payload_hash
        WHERE r.source = 'google_sheets'
          AND s.payload_hash IS NULL
        ORDER BY r.extracted_at, r.id
    """
    
    if limit is not None:
        sql += f" LIMIT {limit}"
    
    try:
        rows = await fetch_one_off(sql)
        logger.info(f"Извлечено {len(rows)} записей из raw.data")
        
        # Преобразовать в формат с parsed payload и hash
        result = []
        for row in rows:
            try:
                # payload хранится как JSON string, нужно распарсить
                payload_dict = json.loads(row['payload'])
                # Use hash from DB if available, otherwise compute
                hash_val = row.get('payload_hash') or payload_hash(payload_dict)
                
                result.append({
                    'raw_id': row['raw_id'], # Now using actual ID from DB (text) or row_number if needed
                    'sheet_row_number': None,
                    'received_at': row['received_at'],
                    'raw_payload': payload_dict,
                    'payload_hash': hash_val
                })
            except Exception as e:
                logger.warning(f"Не удалось распарсить payload для raw_id={row.get('raw_id')}: {e}")
                continue
        
        logger.info(f"Обработано {len(result)} записей с валидным payload")
        return result
        
    except Exception as e:
        logger.error(f"Ошибка запроса записей из raw.data: {e}", exc_info=True)
        return []
