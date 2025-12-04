import pytest
import asyncio
from unittest.mock import MagicMock, patch
from src.extract.google_sheets import fetch_google_sheets
from src.transform.transformer import _normalize_payload, normalize_to_staging
import datetime

@pytest.mark.asyncio
async def test_fetch_google_sheets_padding():
    # Mock the aiohttp session and response
    with patch('aiohttp.ClientSession.get') as mock_get:
        mock_resp = MagicMock()
        mock_resp.json.return_value = asyncio.Future()
        # Return headers + 1 row with fewer columns
        mock_resp.json.return_value.set_result({
            'values': [
                ['Col1', 'Col2'], # Headers
                ['Val1']          # Row with missing value
            ]
        })
        mock_get.return_value.__aenter__.return_value = mock_resp
        
        # Mock token and settings
        with patch('src.extract.google_sheets.get_google_access_token', return_value='fake_token'), \
             patch('src.extract.google_sheets.settings') as mock_settings:
            
            mock_settings.ARCHIVE_PATH = '/tmp'
            
            # Run fetch
            records = await fetch_google_sheets('sheet_id', 'Sheet1!A:AF')
            
            # Check results
            assert len(records) == 1
            row = records[0]
            
            # Check padding of headers (should be 32 columns)
            assert len(row) == 32
            assert 'Column_3' in row # Auto-generated header
            assert 'Column_32' in row
            
            # Check padding of values
            assert row['Col1'] == 'Val1'
            assert row['Col2'] == '' # Padded empty string

def test_normalize_payload_logic():
    # Test number parsing
    payload = {
        'Amount': '1,234.56',
        'Cost': '1.000,50', # European
        'Count': '100',
        'Bad': 'abc',
        'Date': '2023-01-01',
        'Mixed': '123,45'
    }
    
    norm = _normalize_payload(payload)
    
    assert norm['Amount'] == 1234.56
    assert norm['Cost'] == 1000.5
    assert norm['Count'] == 100
    assert norm['Bad'] == 'abc'
    assert norm['Date'] == '2023-01-01T00:00:00+00:00'
    assert norm['Mixed'] == 123.45

@pytest.mark.asyncio
async def test_normalize_to_staging_flow():
    # Mock DB execution
    with patch('src.transform.transformer.executemany_one_off', new_callable=MagicMock) as mock_exec:
        mock_exec.return_value = asyncio.Future()
        mock_exec.return_value.set_result(None)
        
        records = [
            {'id': '1', 'payload': {'Date': '2023-01-01', 'Amount': '100'}}
        ]
        
        await normalize_to_staging('test_source', records)
        
        # Verify SQL execution
        assert mock_exec.call_count == 1
        args = list(mock_exec.call_args[0][1]) # Generator to list
        assert len(args) == 1
        row = args[0]
        
        # Check extracted fields
        # (id, source, created_at, payload, payment_ts, type_text, total_rub_num)
        assert row[0] == '1'
        assert row[6] == 100.0 # total_rub_num extracted from Amount
