from __future__ import annotations

import argparse
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[3]
STREAMLIT_DIR = ROOT_DIR / "streamlit"
if str(STREAMLIT_DIR) not in sys.path:
    sys.path.insert(0, str(STREAMLIT_DIR))

from plugin.postgre.QueryBuilder import Engine  # noqa: E402
from plugin.sheets.UploadSheets import sheetdrive as SheetDrive  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh a Google Sheet tab from a database table.")
    parser.add_argument("--spreadsheet-id", required=True)
    parser.add_argument("--spreadsheet-title", required=False, default="")
    parser.add_argument("--sheet-name", required=True)
    parser.add_argument("--table-name", required=True)
    args = parser.parse_args()

    engine = Engine()
    sheetdrive = SheetDrive()
    query = f"SELECT * FROM {Engine._dataset}.{args.table_name};"
    dataframe = engine.execute_query(query, params=None)

    sheetdrive.update_gsheet(
        spreadsheet_id=args.spreadsheet_id,
        file_name=args.spreadsheet_title or None,
        dataframe=dataframe,
        sheet_name=args.sheet_name,
        overwrite=True,
        open_browser=False,
    )


if __name__ == "__main__":
    main()
