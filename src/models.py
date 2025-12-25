from datetime import datetime
from decimal import Decimal
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


class StagingRecord(BaseModel):
    """Схема данных для staging.records."""

    model_config = ConfigDict(from_attributes=True, arbitrary_types_allowed=True)

    raw_id: str | int
    sheet_row_number: Optional[int] = None
    received_at: datetime
    source_type: str = "live"
    date: Optional[datetime] = None
    payment_date: Optional[datetime] = None
    payment_date_orig: Optional[datetime] = None
    task: Optional[str] = None
    type: Optional[str] = None
    client: Optional[str] = None
    vendor: Optional[str] = None
    cashier: Optional[str] = None
    service: Optional[str] = None
    approver: Optional[str] = None
    category: Optional[str] = None
    currency: Optional[str] = None
    subcategory: Optional[str] = None
    description: Optional[str] = None
    direct_indirect: Optional[str] = None
    cat_new: Optional[str] = None
    cat_final: Optional[str] = None
    subcat_new: Optional[str] = None
    subcat_final: Optional[str] = None
    kategoriya: Optional[str] = None
    podstatya: Optional[str] = None
    statya: Optional[str] = None
    vidy_raskhodov: Optional[str] = None
    paket: Optional[str] = None
    package_secondary: Optional[str] = None
    year: Optional[int] = None
    month: Optional[int] = None
    quarter: Optional[int] = None
    count_vendor: Optional[int] = None
    hours: Optional[Decimal] = None
    fx_rub: Optional[Decimal] = None
    fx_usd: Optional[Decimal] = None
    total_rub: Optional[Decimal] = None
    total_usd: Optional[Decimal] = None
    sum_total_rub: Optional[Decimal] = None
    total_in_currency: Optional[Decimal] = None
    rub_summa: Optional[Decimal] = None
    usd_summa: Optional[Decimal] = None
    payload_hash: str
    raw_payload: dict[str, Any]
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    updated_by: Optional[str] = None

    @field_validator("year", "month", "quarter", mode="before")
    @classmethod
    def empty_string_to_none(cls, v: Any) -> Optional[Any]:
        if v == "":
            return None
        return v
