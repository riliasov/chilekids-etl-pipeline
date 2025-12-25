import asyncio
from unittest.mock import MagicMock, patch

import pytest

from src.sheets import fetch_google_sheets


@pytest.mark.asyncio
async def test_fetch_google_sheets_padding():
    # Mock the aiohttp session and response
    with patch("aiohttp.ClientSession.get") as mock_get:
        mock_resp = MagicMock()
        mock_resp.json.return_value = asyncio.Future()
        # Return headers + 1 row with fewer columns
        mock_resp.json.return_value.set_result(
            {
                "values": [
                    ["Col1", "Col2"],  # Headers
                    ["Val1"],  # Row with missing value
                ]
            }
        )
        mock_get.return_value.__aenter__.return_value = mock_resp

        # Mock token and settings
        with (
            patch("src.sheets.get_google_access_token", return_value="fake_token"),
            patch("src.sheets.settings") as mock_settings,
        ):
            mock_settings.ARCHIVE_PATH = "/tmp"

            # Run fetch
            records = await fetch_google_sheets("sheet_id", "Sheet1!A:AF")

            # Check results
            assert len(records) == 1
            row = records[0]

            # Check padding of headers (should be 32 columns)
            assert len(row) == 32
            assert "Column_3" in row  # Auto-generated header
            assert "Column_32" in row

            # Check padding of values
            assert row["Col1"] == "Val1"
            assert row["Col2"] == ""  # Padded empty string
