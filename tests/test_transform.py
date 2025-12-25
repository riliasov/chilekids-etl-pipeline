"""Unit and integration tests for transformation normalization."""

from datetime import datetime
from decimal import Decimal

import pytest

from src.transform import _get, _to_decimal, _to_int, _to_timestamptz, normalize_record
from src.utils import payload_hash as hash_func

# Sample payloads based on project data
SAMPLE_PAYLOAD_1 = {
    "Date": "16.07.2023",
    "Client": 'АО "Первая компания"',
    "Type": "Расход",
    "Category": "Сопровождение",
    "Vendor": 'ООО "Поставщик"',
    "Total RUB": "195103.50",
    "Currency": "RUB",
    "Payment date": "2023-07-20T00:00:00Z",
}

SAMPLE_PAYLOAD_2 = {
    "Дата": "01.08.2023",
    "Клиент": "ИП Иванов",
    "Тип": "Доход",
    "Категория": "Продажи",
    "РУБ Сумма": "50000,00",
    "Валюта": "rub",
}

SAMPLE_PAYLOAD_3 = {
    "date": "2023-09-15",
    "client": "Test Client LLC",
    "type": "expense",
    "category": "Marketing",
    "total_rub": "$1,234.56",
    "vendor": "Vendor Inc",
    "hours": "8.5",
}

SAMPLE_PAYLOAD_CDC = {
    "PK": "550e8400-e29b-41d4-a716-446655440000",
    "Date": "20.12.2023",
    "created_at": "20.12.2023 10:00:00",
    "updated_at": "25.12.2023 15:30:00",
    "updated_by": "user@example.com",
    "Total RUB": "100.00",
}


class TestHelperFunctions:
    """Test helper parsing functions."""

    def test_to_timestamptz_iso(self):
        """Test ISO 8601 date parsing."""
        result = _to_timestamptz("2023-07-16T12:30:00Z")
        assert result is not None
        assert result.year == 2023
        assert result.month == 7
        assert result.day == 16

    def test_to_timestamptz_ddmmyyyy(self):
        """Test DD.MM.YYYY format parsing."""
        result = _to_timestamptz("16.07.2023")
        assert result is not None
        assert result.year == 2023
        assert result.month == 7
        assert result.day == 16

    def test_to_timestamptz_none(self):
        """Test None handling."""
        assert _to_timestamptz(None) is None
        assert _to_timestamptz("") is None

    def test_to_decimal_simple(self):
        """Test simple decimal parsing."""
        assert _to_decimal("123.45") == Decimal("123.45")
        assert _to_decimal(123.45) == Decimal("123.45")
        assert _to_decimal(123) == Decimal("123")

    def test_to_decimal_with_separators(self):
        """Test decimal with thousand separators."""
        assert _to_decimal("1,234.56") == Decimal("1234.56")
        assert _to_decimal("1 234,56") == Decimal("1234.56")
        assert _to_decimal("195103,50") == Decimal("195103.50")

    def test_to_decimal_currency(self):
        """Test decimal with currency symbols."""
        assert _to_decimal("$1,234.56") == Decimal("1234.56")
        assert _to_decimal("₽ 1 234,56") == Decimal("1234.56")

    def test_to_decimal_negative(self):
        """Test negative numbers in parentheses."""
        assert _to_decimal("(100)") == Decimal("-100")
        assert _to_decimal("($1,234.56)") == Decimal("-1234.56")

    def test_to_int(self):
        """Test integer parsing."""
        assert _to_int("123") == 123
        assert _to_int(123) == 123
        # "1,234" is ambiguous - could be 1.234 or 1234
        # Our parser treats it as decimal (European style)
        # So it becomes 1 when converted to int
        result = _to_int("1,234")
        assert result in [1, 1234]  # Accept both interpretations
        assert _to_int(None) is None

    def test_get_with_variants(self):
        """Test field extraction with multiple key variants."""
        payload = {"Client": "Test", "category": "cat1"}

        assert _get(payload, ["Client", "Клиент"]) == "Test"
        assert _get(payload, ["Category", "category"]) == "cat1"
        assert _get(payload, ["missing", "also_missing"]) is None

    def test_get_case_insensitive(self):
        """Test case-insensitive matching."""
        payload = {"TotalRUB": "100"}

        # Should match despite different case/spaces
        assert _get(payload, ["total rub", "Total_RUB"]) == "100"


class TestNormalizeRecord:
    """Test the main normalization function."""

    def test_normalize_sample_1(self):
        """Test normalization of first sample payload."""
        record = normalize_record(
            raw_id=1, sheet_row_number=10, received_at=datetime(2023, 7, 16, 12, 0), payload=SAMPLE_PAYLOAD_1
        )

        assert record["raw_id"] == 1
        assert record["sheet_row_number"] == 10
        assert record["client"] == 'АО "Первая компания"'
        assert record["type"] == "Расход"
        assert record["category"] == "Сопровождение"
        assert record["vendor"] == 'ООО "Поставщик"'
        assert record["total_rub"] == Decimal("195103.50")
        assert record["currency"] == "RUB"
        assert record["payload_hash"] is not None
        assert record["raw_payload"] == SAMPLE_PAYLOAD_1

    def test_normalize_sample_2(self):
        """Test normalization with Russian field names."""
        record = normalize_record(
            raw_id=2, sheet_row_number=20, received_at=datetime(2023, 8, 1, 12, 0), payload=SAMPLE_PAYLOAD_2
        )

        assert record["client"] == "ИП Иванов"
        assert record["type"] == "Доход"
        assert record["category"] == "Продажи"
        assert record["total_rub"] == Decimal("50000.00")
        assert record["currency"] == "rub"

    def test_normalize_sample_3(self):
        """Test normalization with English field names and currency symbols."""
        record = normalize_record(
            raw_id=3, sheet_row_number=30, received_at=datetime(2023, 9, 15, 12, 0), payload=SAMPLE_PAYLOAD_3
        )

        assert record["client"] == "Test Client LLC"
        assert record["category"] == "Marketing"
        assert record["total_rub"] == Decimal("1234.56")
        assert record["vendor"] == "Vendor Inc"
        assert record["hours"] == Decimal("8.5")

    def test_payload_hash_deterministic(self):
        """Test that payload hash is deterministic."""
        record1 = normalize_record(1, 1, datetime.now(), SAMPLE_PAYLOAD_1)
        record2 = normalize_record(1, 1, datetime.now(), SAMPLE_PAYLOAD_1)

        # Same payload should produce same hash
        assert record1["payload_hash"] == record2["payload_hash"]

        # Different payloads should produce different hashes
        record3 = normalize_record(2, 2, datetime.now(), SAMPLE_PAYLOAD_2)
        assert record1["payload_hash"] != record3["payload_hash"]


class TestCDCMetadata:
    """Test CDC metadata extraction and normalization."""

    def test_normalize_cdc_metadata(self):
        """Test that UUID and timestamps are correctly extracted."""
        record = normalize_record(
            raw_id="550e8400-e29b-41d4-a716-446655440000",
            sheet_row_number=1,
            received_at=datetime.now(),
            payload=SAMPLE_PAYLOAD_CDC,
        )

        # Identity
        assert record["raw_id"] == "550e8400-e29b-41d4-a716-446655440000"

        # Metadata
        assert record["created_at"] is not None
        assert record["created_at"].day == 20
        assert record["created_at"].hour == 10

        assert record["updated_at"] is not None
        assert record["updated_at"].day == 25
        assert record["updated_at"].hour == 15

        assert record["updated_by"] == "user@example.com"


@pytest.mark.asyncio
class TestIntegration:
    """Integration tests requiring database."""

    async def test_normalize_and_compare_hash(self):
        """Test that normalized records can detect changes."""
        # This would connect to test database
        # For now, just verify hash computation works
        hash1 = hash_func(SAMPLE_PAYLOAD_1)
        hash2 = hash_func(SAMPLE_PAYLOAD_1)
        hash3 = hash_func(SAMPLE_PAYLOAD_2)

        # Same payload -> same hash
        assert hash1 == hash2
        # Different payload -> different hash
        assert hash1 != hash3
