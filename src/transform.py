import datetime
import json
import logging
from decimal import Decimal, InvalidOperation
from typing import Any

from dateutil import parser as dateutil_parser

from .db import fetch_one_off, get_db_pool
from .models import StagingRecord
from .utils import payload_hash

logger = logging.getLogger(__name__)

# --- Normalizer Helpers ---


def _to_timestamptz(val: Any) -> datetime.datetime | None:
    if val is None or val == "":
        return None
    if isinstance(val, datetime.datetime):
        return val.replace(tzinfo=datetime.UTC) if val.tzinfo is None else val.astimezone(datetime.UTC)
    try:
        dt = dateutil_parser.isoparse(str(val))
        return dt.replace(tzinfo=datetime.UTC) if dt.tzinfo is None else dt.astimezone(datetime.UTC)
    except (ValueError, TypeError):
        pass
    for fmt in [
        "%d.%m.%Y %H:%M:%S",
        "%d.%m.%Y",
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%d/%m/%Y",
    ]:
        try:
            dt = datetime.datetime.strptime(str(val), fmt)
            return dt.replace(tzinfo=datetime.UTC)
        except (ValueError, TypeError):
            continue
    return None


def _clean_numeric_string(s: str) -> tuple[str, bool]:
    s = s.strip()
    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1].strip()
    s = s.replace("$", "").replace("€", "").replace("₽", "").replace("\xa0", "").replace(" ", "")
    return s, neg


def _fix_separators(s: str) -> str:
    if "," in s and "." in s:
        if s.rfind(".") > s.rfind(","):
            s = s.replace(",", "")
        else:
            s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        parts = s.split(",")
        if len(parts) == 2 and len(parts[1]) <= 3:
            s = s.replace(",", ".")
        else:
            s = s.replace(",", "")
    return s


def _to_decimal(val: Any) -> Decimal | None:
    if val is None or val == "":
        return None
    if isinstance(val, Decimal):
        return val
    if isinstance(val, (int, float)):
        return Decimal(str(val))

    s, neg = _clean_numeric_string(str(val))
    if s == "":
        return None

    s = _fix_separators(s)

    try:
        result = Decimal(s)
        return -result if neg else result
    except (InvalidOperation, ValueError):
        return None


def _to_int(val: Any) -> int | None:
    if val is None or val == "":
        return None
    if isinstance(val, int):
        return val
    if isinstance(val, float):
        return int(val)
    try:
        dec = _to_decimal(val)
        if dec is not None:
            return int(dec)
    except Exception:
        pass
    try:
        return int(str(val).strip())
    except (ValueError, TypeError):
        return None


def _get(payload: dict[str, Any], key_variants: list[str]) -> Any:
    for key in key_variants:
        if key in payload:
            return payload[key]
    normalized_payload = {k.lower().replace(" ", ""): v for k, v in payload.items()}
    for key in key_variants:
        normalized_key = key.lower().replace(" ", "")
        if normalized_key in normalized_payload:
            return normalized_payload[normalized_key]
    return None


# --- Normalizer Core ---


def normalize_record(
    raw_id: int,
    sheet_row_number: int,
    received_at: datetime.datetime,
    payload: dict[str, Any],
    source_type: str = "live",
) -> dict[str, Any]:
    hash_value = payload_hash(payload)
    data = {
        "raw_id": raw_id,
        "sheet_row_number": sheet_row_number,
        "received_at": received_at,
        "source_type": source_type,
        "date": _to_timestamptz(_get(payload, ["Date", "Дата", "date"])),
        "payment_date": _to_timestamptz(
            _get(payload, ["Payment date", "Payment Date", "Дата платежа", "payment_date"])
        ),
        "payment_date_orig": _to_timestamptz(
            _get(payload, ["Payment date (orig)", "Дата платежа (ориг)", "payment_date_orig"])
        ),
        "task": _get(payload, ["Task", "Задача", "task"]),
        "type": _get(payload, ["Type", "Тип", "type"]),
        "client": _get(payload, ["Client", "Клиент", "client"]),
        "vendor": _get(payload, ["Vendor", "Поставщик", "vendor"]),
        "cashier": _get(payload, ["Cashier", "Кассир", "cashier"]),
        "service": _get(payload, ["Service", "Услуга", "service"]),
        "approver": _get(payload, ["Approver", "Утверждающий", "approver"]),
        "category": _get(payload, ["Category", "Категория", "category"]),
        "currency": _get(payload, ["Currency", "Валюта", "currency"]),
        "subcategory": _get(payload, ["Subcategory", "Подкатегория", "subcategory"]),
        "description": _get(payload, ["Description", "Описание", "description"]),
        "direct_indirect": _get(payload, ["Direct/Indirect", "Прямые/Косвенные", "direct_indirect"]),
        "cat_new": _get(payload, ["cat_new", "Категория новая"]),
        "cat_final": _get(payload, ["cat_final", "Категория финал"]),
        "subcat_new": _get(payload, ["subcat_new", "Подкатегория новая"]),
        "subcat_final": _get(payload, ["subcat_final", "Подкатегория финал"]),
        "kategoriya": _get(payload, ["kategoriya", "Категория"]),
        "podstatya": _get(payload, ["podstatya", "Подстатья"]),
        "statya": _get(payload, ["statya", "Статья"]),
        "vidy_raskhodov": _get(payload, ["vidy_raskhodov", "Виды расходов"]),
        "paket": _get(payload, ["paket", "Пакет", "package"]),
        "package_secondary": _get(payload, ["package_secondary", "package secondary", "Пакет вторичный"]),
        "year": _to_int(_get(payload, ["Year", "Год", "year"])),
        "month": _to_int(_get(payload, ["Month", "Месяц", "month"])),
        "quarter": _to_int(_get(payload, ["Quarter", "Квартал", "quarter"])),
        "count_vendor": _to_int(_get(payload, ["Count vendor", "Количество поставщиков", "count_vendor"])),
        "hours": _to_decimal(_get(payload, ["Hours", "Часы", "hours"])),
        "fx_rub": _to_decimal(_get(payload, ["FX RUB", "Курс РУБ", "fx_rub"])),
        "fx_usd": _to_decimal(_get(payload, ["FX USD", "Курс USD", "fx_usd"])),
        "total_rub": _to_decimal(_get(payload, ["Total RUB", "РУБ сумма", "total_rub", "rub_summa", "РУБ Сумма"])),
        "total_usd": _to_decimal(_get(payload, ["Total USD", "USD сумма", "total_usd", "usd_summa"])),
        "sum_total_rub": _to_decimal(_get(payload, ["sum Total RUB", "Сумма РУБ", "sum_total_rub"])),
        "total_in_currency": _to_decimal(_get(payload, ["Total in currency", "Сумма в валюте", "total_in_currency"])),
        "rub_summa": _to_decimal(_get(payload, ["rub_summa", "РУБ Сумма"])),
        "usd_summa": _to_decimal(_get(payload, ["usd_summa", "USD Сумма"])),
        "created_at": _to_timestamptz(_get(payload, ["created_at"])),
        "updated_at": _to_timestamptz(_get(payload, ["updated_at"])),
        "updated_by": _get(payload, ["updated_by"]),
        "payload_hash": hash_value,
        "raw_payload": payload,
    }

    # Validate with Pydantic
    validated = StagingRecord(**data)
    result = validated.model_dump()

    # Financial check (legacy warning compatibility)
    if result.get("type") in ["Доход", "Расход", "Income", "Expense"]:
        if result.get("total_rub") is None:
            logger.warning(
                f"⚠️ Validation Warning: ID={raw_id} (row={sheet_row_number}) "
                f"is '{result.get('type')}' but 'Total RUB' is missing/invalid."
            )

    return result


async def get_changed_raw_records(
    source: str = "google_sheets", limit: int | None = None, batch_size: int = 500
) -> list[dict[str, Any]]:
    sql = f"""
        SELECT r.id as raw_id, r.extracted_at as received_at, r.payload, r.payload_hash
        FROM raw.data r
        LEFT JOIN staging.records s ON r.payload_hash = s.payload_hash
        WHERE r.source = '{source}' AND s.payload_hash IS NULL
        ORDER BY r.extracted_at, r.id
    """
    if limit is not None:
        sql += f" LIMIT {limit}"

    try:
        rows = await fetch_one_off(sql)
        result = []
        for row in rows:
            try:
                payload_dict = json.loads(row["payload"])
                hash_val = row.get("payload_hash") or payload_hash(payload_dict)
                result.append(
                    {
                        "raw_id": row["raw_id"],
                        "sheet_row_number": None,
                        "received_at": row["received_at"],
                        "raw_payload": payload_dict,
                        "payload_hash": hash_val,
                    }
                )
            except Exception:
                continue
        return result
    except Exception as e:
        logger.error(f"Ошибка запроса записей из raw.data: {e}", exc_info=True)
        return []


# --- Loader ---


async def upsert_staging_records(records: list[dict[str, Any]]) -> int:
    if not records:
        return 0
    fields = [
        "raw_id",
        "sheet_row_number",
        "received_at",
        "source_type",
        "date",
        "payment_date",
        "task",
        "type",
        "year",
        "hours",
        "month",
        "client",
        "fx_rub",
        "fx_usd",
        "vendor",
        "cashier",
        "cat_new",
        "quarter",
        "service",
        "approver",
        "category",
        "currency",
        "cat_final",
        "total_rub",
        "total_usd",
        "subcat_new",
        "paket",
        "description",
        "subcategory",
        "payment_date_orig",
        "subcat_final",
        "count_vendor",
        "statya",
        "sum_total_rub",
        "usd_summa",
        "direct_indirect",
        "package_secondary",
        "total_in_currency",
        "rub_summa",
        "kategoriya",
        "podstatya",
        "vidy_raskhodov",
        "payload_hash",
        "raw_payload",
        "created_at",
        "updated_at",
        "updated_by",
    ]
    placeholders = ", ".join(f"${i + 1}" for i in range(len(fields)))
    field_list = ", ".join(fields)
    update_fields = [f for f in fields if f != "raw_id"]
    update_clause = ", ".join(f"{f} = EXCLUDED.{f}" for f in update_fields)

    sql = (
        f"INSERT INTO staging.records ({field_list}) VALUES ({placeholders}) "
        f"ON CONFLICT (raw_id) DO UPDATE SET {update_clause}"
    )

    pool = get_db_pool()
    if pool is None:
        raise RuntimeError("Database pool not initialized")
    successful = 0
    async with pool.acquire() as conn:
        prepared_records = []
        for record in records:
            try:
                record_copy = record.copy()
                if "raw_payload" in record_copy and isinstance(record_copy["raw_payload"], dict):
                    record_copy["raw_payload"] = json.dumps(record_copy["raw_payload"])
                prepared_records.append(tuple(record_copy.get(f) for f in fields))
            except Exception:
                pass

        if not prepared_records:
            return 0
        try:
            async with conn.transaction():
                await conn.executemany(sql, prepared_records)
                successful = len(prepared_records)
        except Exception:
            # Batch failed, fallback to row-by-row
            logger.warning("Batch insert failed, falling back to row-by-row insert.")
            async with conn.transaction():
                for values in prepared_records:
                    try:
                        await conn.execute(sql, *values)
                        successful += 1
                    except Exception as e:
                        # Log specific error for the record
                        logger.error(f"Failed to upsert record: {e}")
    return successful


async def upsert_staging_records_batch(records: list[dict[str, Any]], batch_size: int = 100) -> int:
    if not records:
        return 0
    total_upserted = 0
    for i in range(0, len(records), batch_size):
        try:
            total_upserted += await upsert_staging_records(records[i : i + batch_size])
        except Exception:
            continue
    return total_upserted
