import datetime as dt

import pytest

from drivestats.metrics import afr_pct
from part_d.mongo_pipeline import parse_args, window_documents


def test_window_documents_one_per_model_and_window_with_utc_dates():
    rows = [
        ("2026-04-01", "2026-04-08", "HGST HUH721212ALN604", 67_082, 22),
        ("2026-04-08", "2026-04-15", "HGST HUH721212ALN604", 66_990, 0),
    ]
    first, second = window_documents(rows)
    assert first == {
        "model": "HGST HUH721212ALN604",
        "window_start": dt.datetime(2026, 4, 1, tzinfo=dt.UTC),
        "window_end": dt.datetime(2026, 4, 8, tzinfo=dt.UTC),
        "total_days": 67_082,
        "total_failures": 22,
        "afr": afr_pct(22, 67_082),
    }
    assert round(first["afr"], 2) == 11.97  # el mismo valor que la Parte A y la Parte C
    assert second["window_start"] == first["window_end"]
    assert second["afr"] == 0


def test_parse_args_defaults_match_part_c_windows():
    args = parse_args([])
    assert (args.window_days, args.min_drive_days) == (7, 100)
    with pytest.raises(SystemExit):
        parse_args(["--window-days", "0"])
