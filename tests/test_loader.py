"""Tests for JSONB serialization in loader.py"""
import pytest
import json
from datetime import datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

from src.transform import upsert_staging_records


class TestJSONBSerialization:
    """Test JSONB serialization for raw_payload field."""
    
    def test_jsonb_serialization_dict(self):
        """Test that dict payload is serialized to JSON string."""
        record = {
            'raw_id': 1,
            'raw_payload': {'key': 'value', 'number': 123}
        }
        
        # Serialize as loader does
        record_copy = record.copy()
        if 'raw_payload' in record_copy and isinstance(record_copy['raw_payload'], dict):
            record_copy['raw_payload'] = json.dumps(record_copy['raw_payload'])
        
        # Verify it's now a string
        assert isinstance(record_copy['raw_payload'], str)
        # Verify it's valid JSON
        parsed = json.loads(record_copy['raw_payload'])
        assert parsed == {'key': 'value', 'number': 123}
    
    def test_jsonb_serialization_already_string(self):
        """Test that already serialized JSON string is not double-encoded."""
        json_str = '{"key": "value"}'
        record = {
            'raw_id': 1,
            'raw_payload': json_str
        }
        
        # Serialize as loader does
        record_copy = record.copy()
        if 'raw_payload' in record_copy and isinstance(record_copy['raw_payload'], dict):
            record_copy['raw_payload'] = json.dumps(record_copy['raw_payload'])
        
        # Should remain unchanged
        assert record_copy['raw_payload'] == json_str
    
    def test_jsonb_serialization_none(self):
        """Test that None payload is handled correctly."""
        record = {
            'raw_id': 1,
            'raw_payload': None
        }
        
        # Serialize as loader does
        record_copy = record.copy()
        if 'raw_payload' in record_copy and isinstance(record_copy['raw_payload'], dict):
            record_copy['raw_payload'] = json.dumps(record_copy['raw_payload'])
        
        # Should remain None
        assert record_copy['raw_payload'] is None
    
    def test_jsonb_serialization_complex_payload(self):
        """Test serialization of complex nested payload."""
        complex_payload = {
            'Date': '16.07.2023',
            'Client': 'Test Company',
            'Total RUB': '195103.50',
            'nested': {
                'array': [1, 2, 3],
                'bool': True,
                'null': None
            }
        }
        
        record = {
            'raw_id': 1,
            'raw_payload': complex_payload
        }
        
        # Serialize
        record_copy = record.copy()
        if 'raw_payload' in record_copy and isinstance(record_copy['raw_payload'], dict):
            record_copy['raw_payload'] = json.dumps(record_copy['raw_payload'])
        
        # Verify serialization
        assert isinstance(record_copy['raw_payload'], str)
        parsed = json.loads(record_copy['raw_payload'])
        assert parsed == complex_payload


@pytest.mark.asyncio
class TestUpsertWithJSONB:
    """Integration tests for upsert with JSONB payload."""
    
    async def test_upsert_with_dict_payload(self):
        """Test that upsert correctly handles dict payload."""
        # Mock database pool and connection
        mock_conn = MagicMock()  # Connection object itself is not awaitable, its methods are
        mock_conn.execute = AsyncMock()
        
        # Mock transaction context manager
        mock_transaction = AsyncMock()
        mock_transaction.__aenter__.return_value = None
        mock_transaction.__aexit__.return_value = None
        mock_conn.transaction.return_value = mock_transaction

        mock_pool = MagicMock()
        # pool.acquire() is an async context manager
        mock_pool_ctx = AsyncMock()
        mock_pool_ctx.__aenter__.return_value = mock_conn
        mock_pool_ctx.__aexit__.return_value = None
        mock_pool.acquire.return_value = mock_pool_ctx
        
        # Sample record with dict payload
        records = [{
            'raw_id': 1,
            'sheet_row_number': 10,
            'received_at': datetime(2023, 7, 16),
            'date': datetime(2023, 7, 16),
            'payment_date': datetime(2023, 7, 20),
            'task': None,
            'type': 'Доход',
            'year': None,
            'hours': None,
            'month': None,
            'client': 'Test Client',
            'fx_rub': None,
            'fx_usd': None,
            'vendor': 'Test Vendor',
            'cashier': None,
            'cat_new': None,
            'quarter': None,
            'service': None,
            'approver': None,
            'category': 'Доходы',
            'currency': 'RUB',
            'cat_final': None,
            'total_rub': Decimal('100000'),
            'total_usd': None,
            'subcat_new': None,
            'paket': None,
            'description': None,
            'subcategory': None,
            'payment_date_orig': None,
            'subcat_final': None,
            'count_vendor': None,
            'statya': None,
            'sum_total_rub': None,
            'usd_summa': None,
            'direct_indirect': None,
            'package_secondary': None,
            'total_in_currency': None,
            'rub_summa': None,
            'kategoriya': None,
            'podstatya': None,
            'vidy_raskhodov': None,
            'payload_hash': 'abc123',
            'raw_payload': {'Date': '16.07.2023', 'Client': 'Test Client'}  # Dict payload
        }]
        
        with patch('src.transform.get_db_pool', return_value=mock_pool):
            result = await upsert_staging_records(records)
        
        # Verify upsert was called
        assert mock_conn.execute.called
        # Verify result
        assert result == 1
        
        # Verify that raw_payload was serialized to JSON string
        call_args = mock_conn.execute.call_args
        # The raw_payload should be a JSON string in the arguments
        args = call_args[0]
        # Find raw_payload in args (it's the last argument, position 42)
        raw_payload_arg = args[-1]
        assert isinstance(raw_payload_arg, str)
        parsed = json.loads(raw_payload_arg)
        assert parsed == {'Date': '16.07.2023', 'Client': 'Test Client'}
