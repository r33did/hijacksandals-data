import datetime

import streamlit as st

from plugin.postgre.QueryBuilder import Engine
from plugin.sheets.StreamlitGoogleAuth import (
    get_google_drive_creds,
    is_google_drive_connected,
)
from plugin.sheets.UploadSheets import sheetdrive as SheetDrive

engine = Engine()
sheetdrive = SheetDrive()


def configure_sheetdrive(creds=None, folder_id: str | None = None):
    global sheetdrive
    sheetdrive = SheetDrive(creds=creds)
    if folder_id:
        sheetdrive.set_main_id(folder_id)


@st.cache_data(ttl=300)
def get_tables():
    return engine.get_all_tables()


@st.cache_data(ttl=300)
def get_columns(table_name):
    return engine.get_table_columns(table_name)


tables = get_tables()

if not tables:
    try:
        engine.ping()
    except Exception as e:
        st.error(f"Failed to connect to database: {e}")
        st.stop()


def init_session_state():
    defaults = {
        "filters": [],
        "db_select": None,
        "extract_result": None,
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def render_extract_result(active_table_name: str | None):
    extract_result = st.session_state.get("extract_result")
    if not extract_result or extract_result.get("table_name") != active_table_name:
        return

    dataframe = extract_result.get("dataframe")
    if dataframe is None or dataframe.empty:
        st.info("No data returned from query.")
        return

    st.success(f"Successfully retrieved {len(dataframe)} rows!")
    st.caption("Data only shows first 50 rows")
    st.dataframe(dataframe.head(50), use_container_width=True)

    with st.expander("Show Generated SQL"):
        st.code(
            f"Generated Query:\n{extract_result.get('used_query')}\n\nParams: {extract_result.get('params')}",
            language="sql",
        )

    csv = dataframe.to_csv(index=False).encode("utf-8")
    op1, op2 = st.columns(2)
    with op1:
        st.download_button(
            label="Download Data as CSV",
            data=csv,
            file_name=f"{active_table_name}_extract.csv",
            mime="text/csv",
            use_container_width=True,
        )
    with op2:
        if not is_google_drive_connected():
            st.button(
                label="Upload To Spreadsheet",
                use_container_width=True,
                disabled=True,
            )
            st.caption("Connect Google Drive first using the sidebar button.")
        elif st.button(
            label="Upload To Spreadsheet",
            use_container_width=True,
        ):
            try:
                authorized_sheetdrive = SheetDrive(creds=get_google_drive_creds())
                authorized_sheetdrive.set_main_id(sheetdrive.main_id)
                spreadsheet_url = authorized_sheetdrive.upload_new_gsheet(dataframe=dataframe)
            except Exception as exc:
                st.error(f"Failed to upload spreadsheet: {exc}")
            else:
                st.success("Spreadsheet created successfully.")
                st.markdown(f"[Open spreadsheet]({spreadsheet_url})")


def extract_page():
    st.title("Data Extractor", anchor="center")
    with st.expander("How this page works", expanded=True):
        st.markdown(
            "\n".join(
                [
                    "1. Pick one PostgreSQL table.",
                    "2. Add optional date filters and column filters.",
                    "3. Run the query and review the generated SQL.",
                    "4. Download the result as CSV or upload it to a new spreadsheet.",
                ]
            )
        )

    st.caption(
        "Google Drive status: "
        + (
            "Connected"
            if is_google_drive_connected()
            else "Not connected. Use the sidebar button to connect before uploading to spreadsheet."
        )
    )

    init_session_state()
    table_name = st.selectbox("Database Table", tables, placeholder="Select Table", key="db_select")

    if table_name:
        columns_dict = get_columns(table_name)
        column_names = list(columns_dict.keys())

        if len(column_names) > 0:
            st.toast(f"Total Columns {len(column_names)}, Found!", icon="✅")
        else:
            st.error("Failed to fetch columns")

        st.markdown("**Filter by Date (Optional)**")
        date_cols = [
            column
            for column, column_type in columns_dict.items()
            if "date" in column_type.lower() or "time" in column_type.lower()
        ]
        disabled = not bool(date_cols)

        d_col1, d_col2, d_col3 = st.columns(3)
        with d_col1:
            date_column = st.selectbox("Date Column", ["None"] + date_cols)
        with d_col2:
            start_date = st.date_input(
                "Start Date",
                value=datetime.date.today() - datetime.timedelta(days=30),
                disabled=disabled,
            )
        with d_col3:
            end_date = st.date_input("End Date", value=datetime.date.today(), disabled=disabled)

        st.markdown("**Filter by Columns (Optional)**")

        if st.button("Add Filter / WHERE Statement"):
            st.session_state.filters.append({"column": column_names[0], "operator": "=", "value": ""})
            st.rerun()

        filters_to_remove = []
        updated_conditions = []
        repr_conditions = []

        for i, _filter in enumerate(st.session_state.filters):
            f_col1, f_col2, f_col3, f_col4 = st.columns([3, 0.8, 4, 0.5])

            with f_col1:
                idx_col = column_names.index(_filter["column"]) if _filter["column"] in column_names else 0
                sel_col = st.selectbox(f"Column {i + 1}", column_names, index=idx_col, key=f"col_{i}")
                col_type = columns_dict.get(sel_col, "text")

            with f_col2:
                if "char" in col_type or "text" in col_type:
                    ops = ["=", "!=", "LIKE", "ILIKE", "IN"]
                elif "int" in col_type or "numeric" in col_type or "real" in col_type or "float" in col_type:
                    ops = ["=", "!=", ">", "<", ">=", "<="]
                else:
                    ops = ["=", "!="]

                current_op = _filter["operator"] if _filter["operator"] in ops else ops[0]
                sel_op = st.selectbox("Operator", ops, index=ops.index(current_op), key=f"op_{i}")

            with f_col3:
                if "int" in col_type or "numeric" in col_type or "real" in col_type or "float" in col_type:
                    try:
                        val_placeholder = float(_filter["value"]) if _filter["value"] != "" else 0.0
                    except ValueError:
                        val_placeholder = 0.0
                    sel_val = st.number_input("Value", value=val_placeholder, key=f"val_{i}")
                else:
                    sel_val = st.text_input("Value", value=str(_filter["value"]), key=f"val_{i}")

            with f_col4:
                st.markdown("<br>", unsafe_allow_html=True)
                if st.button("X", key=f"del_{i}"):
                    filters_to_remove.append(i)

            updated_conditions.append(
                {
                    "column": sel_col,
                    "operator": sel_op,
                    "value": sel_val,
                    "type": col_type,
                }
            )

            repr_conditions.append(
                {
                    "column": f'"{sel_col}"',
                    "operator": sel_op,
                    "value": sel_val,
                    "type": col_type,
                }
            )

        if filters_to_remove:
            for idx in reversed(filters_to_remove):
                updated_conditions.pop(idx)
                repr_conditions.pop(idx)
            st.session_state.filters = updated_conditions
            st.session_state.repr_filters = repr_conditions
            st.rerun()
        else:
            st.session_state.filters = updated_conditions
            st.session_state.repr_filters = repr_conditions

        st.markdown("---")

        if st.button("Get Table", type="primary", use_container_width=True):
            with st.spinner("Fetching data from PostgreSQL..."):
                active_date_col = date_column if date_column != "None" else None
                str_start_date = start_date.strftime("%Y-%m-%d") if active_date_col else None
                str_end_date = end_date.strftime("%Y-%m-%d") if active_date_col else None

                final_conditions = []
                repr_final_conditions = []
                for condition in updated_conditions:
                    if condition["operator"] == "IN" and isinstance(condition["value"], str):
                        condition["value"] = tuple(
                            [item.strip() for item in condition["value"].split(",") if item.strip()]
                        )
                    final_conditions.append(condition)

                for condition in repr_conditions:
                    if condition["operator"] == "IN" and isinstance(condition["value"], str):
                        condition["value"] = tuple(
                            [item.strip() for item in condition["value"].split(",") if item.strip()]
                        )
                    repr_final_conditions.append(condition)

                query, params = engine.build_dynamic_query(
                    table_name=table_name,
                    date_column=active_date_col,
                    start_date=str_start_date,
                    end_date=str_end_date,
                    conditions=final_conditions,
                )

                repr_query, _ = engine.build_dynamic_query(
                    table_name=table_name,
                    date_column=active_date_col,
                    start_date=str_start_date,
                    end_date=str_end_date,
                    conditions=repr_final_conditions,
                )

                dataframe = engine.execute_query(query=query, params=tuple(params))
                used_query = query

                if dataframe.empty:
                    st.toast("Query returned no rows, trying alternative query", icon="❗")
                    dataframe = engine.execute_query(query=repr_query, params=tuple(params))
                    used_query = repr_query

                st.session_state.extract_result = {
                    "table_name": table_name,
                    "dataframe": dataframe,
                    "used_query": used_query,
                    "params": params,
                }

        render_extract_result(table_name)
