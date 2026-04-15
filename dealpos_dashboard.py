"""
DealPOS Interactive Report Dashboard
=====================================
Covers: Sales Report | Inventory Check | Logistics

HOW TO RUN:
  1. pip install streamlit plotly pandas requests
  2. streamlit run dealpos_dashboard.py
"""

import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json

# ─────────────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="DealPOS Report Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────
# CUSTOM CSS
# ─────────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600;700&display=swap');

    html, body, [class*="css"] {
        font-family: 'IBM Plex Sans', sans-serif;
    }

    /* Dark sidebar */
    [data-testid="stSidebar"] {
        background: #0f172a;
        border-right: 1px solid #1e293b;
    }
    [data-testid="stSidebar"] * {
        color: #cbd5e1 !important;
    }
    [data-testid="stSidebar"] .stSelectbox label,
    [data-testid="stSidebar"] .stDateInput label,
    [data-testid="stSidebar"] .stTextInput label {
        color: #94a3b8 !important;
        font-size: 11px !important;
        text-transform: uppercase;
        letter-spacing: 1px;
    }

    /* Main background */
    .main .block-container {
        background: #0f172a;
        padding-top: 1.5rem;
    }
    .stApp {
        background: #0f172a;
    }

    /* Metric cards */
    [data-testid="stMetric"] {
        background: #1e293b;
        border: 1px solid #334155;
        border-radius: 12px;
        padding: 16px 20px;
    }
    [data-testid="stMetric"] label {
        color: #64748b !important;
        font-size: 11px !important;
        text-transform: uppercase;
        letter-spacing: 1.5px;
        font-weight: 600;
    }
    [data-testid="stMetricValue"] {
        color: #f1f5f9 !important;
        font-family: 'IBM Plex Mono', monospace !important;
        font-size: 1.6rem !important;
        font-weight: 600;
    }
    [data-testid="stMetricDelta"] {
        font-family: 'IBM Plex Mono', monospace !important;
        font-size: 12px !important;
    }

    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        background: #1e293b;
        border-radius: 10px;
        padding: 4px;
        gap: 4px;
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px;
        color: #64748b;
        font-weight: 600;
        font-size: 13px;
        letter-spacing: 0.5px;
        padding: 8px 20px;
    }
    .stTabs [aria-selected="true"] {
        background: #0ea5e9 !important;
        color: white !important;
    }

    /* Headings */
    h1, h2, h3 {
        color: #f1f5f9 !important;
    }
    h1 {
        font-size: 1.8rem !important;
        font-weight: 700;
        letter-spacing: -0.5px;
    }

    /* Dataframe */
    [data-testid="stDataFrame"] {
        border: 1px solid #1e293b;
        border-radius: 10px;
    }

    /* Divider */
    hr {
        border-color: #1e293b;
    }

    /* Section label */
    .section-label {
        font-size: 10px;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 2px;
        color: #0ea5e9;
        margin-bottom: 4px;
    }

    /* Status badges */
    .badge-success { color: #10b981; font-weight: 600; }
    .badge-warning { color: #f59e0b; font-weight: 600; }
    .badge-danger  { color: #ef4444; font-weight: 600; }

    /* Info box */
    .info-box {
        background: #1e293b;
        border-left: 3px solid #0ea5e9;
        border-radius: 6px;
        padding: 12px 16px;
        margin: 8px 0;
        color: #94a3b8;
        font-size: 13px;
    }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────
# SESSION STATE
# ─────────────────────────────────────────────────────────────
if "token" not in st.session_state:
    st.session_state.token = None
if "subdomain" not in st.session_state:
    st.session_state.subdomain = ""


# ─────────────────────────────────────────────────────────────
# API HELPERS
# ─────────────────────────────────────────────────────────────
def base_url():
    return f"https://{st.session_state.subdomain}/api/v3"


def get_token(subdomain, client_id, client_secret):
    url = f"https://{subdomain}/api/v3/Token/OAuth2"
    res = requests.post(url, json={
        "ClientID": client_id,
        "ClientSecret": client_secret,
        "GrantType": "client_credentials"
    }, timeout=15)
    res.raise_for_status()
    data = res.json()
    return data.get("access_token") or data.get("AccessToken") or data.get("token") or data.get("Token")


def api_get(path, params=None):
    headers = {"Authorization": f"Bearer {st.session_state.token}"}
    url = f"{base_url()}{path}"
    res = requests.get(url, headers=headers, params=params or {}, timeout=15)
    res.raise_for_status()
    return res.json()


def safe_get(path, params=None, label="data"):
    try:
        return api_get(path, params)
    except Exception as e:
        st.warning(f"⚠️ Could not load {label}: {e}")
        return None


# ─────────────────────────────────────────────────────────────
# DATA HELPERS
# ─────────────────────────────────────────────────────────────
def to_df(data, keys=None):
    """Safely convert API response to DataFrame."""
    if data is None:
        return pd.DataFrame()
    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        # Try common envelope keys
        for k in (keys or ["Data", "data", "Result", "result", "Items", "items", ""]):
            if k and k in data:
                rows = data[k]
                break
        else:
            rows = [data]
    else:
        return pd.DataFrame()
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def fmt_idr(val):
    try:
        return f"Rp {int(val):,.0f}"
    except Exception:
        return str(val)


# ─────────────────────────────────────────────────────────────
# SIDEBAR – CREDENTIALS & FILTERS
# ─────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚙️ DealPOS Config")
    st.markdown("---")

    st.markdown('<div class="section-label">Connection</div>', unsafe_allow_html=True)
    subdomain = st.text_input("Subdomain", placeholder="yourbrand.dealpos.net", value=st.session_state.subdomain)
    client_id = st.text_input("Client ID", type="password")
    client_secret = st.text_input("Client Secret", type="password")

    if st.button("🔑 Connect", use_container_width=True):
        if subdomain and client_id and client_secret:
            with st.spinner("Authenticating..."):
                try:
                    token = get_token(subdomain, client_id, client_secret)
                    st.session_state.token = token
                    st.session_state.subdomain = subdomain
                    st.success("✅ Connected!")
                except Exception as e:
                    st.error(f"❌ Auth failed: {e}")
        else:
            st.warning("Fill in all fields.")

    if st.session_state.token:
        st.markdown('<div style="color:#10b981;font-size:12px;font-weight:600;">● CONNECTED</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div style="color:#ef4444;font-size:12px;font-weight:600;">● NOT CONNECTED</div>', unsafe_allow_html=True)

    st.markdown("---")
    st.markdown('<div class="section-label">Date Range</div>', unsafe_allow_html=True)

    preset = st.selectbox("Quick Range", ["Last 7 Days", "Last 30 Days", "Last 90 Days", "Custom"])
    today = datetime.today().date()

    if preset == "Last 7 Days":
        date_from = today - timedelta(days=7)
        date_to = today
    elif preset == "Last 30 Days":
        date_from = today - timedelta(days=30)
        date_to = today
    elif preset == "Last 90 Days":
        date_from = today - timedelta(days=90)
        date_to = today
    else:
        date_from = st.date_input("From", today - timedelta(days=30))
        date_to = st.date_input("To", today)

    date_from_str = date_from.strftime("%Y-%m-%d")
    date_to_str = date_to.strftime("%Y-%m-%d")

    st.markdown("---")
    st.markdown('<div class="section-label">Outlet</div>', unsafe_allow_html=True)
    outlet_code = st.text_input("Outlet Code (optional)", placeholder="All outlets")

    st.markdown("---")
    st.markdown('<div class="section-label">Display</div>', unsafe_allow_html=True)
    currency = st.selectbox("Currency Label", ["IDR", "USD", "SGD"])
    low_stock_threshold = st.number_input("Low Stock Threshold", min_value=0, value=10)


# ─────────────────────────────────────────────────────────────
# MAIN HEADER
# ─────────────────────────────────────────────────────────────
col_h1, col_h2 = st.columns([3, 1])
with col_h1:
    st.markdown("# 📊 DealPOS Report Dashboard")
    st.markdown(f'<div style="color:#64748b;font-size:13px;">Period: <b style="color:#0ea5e9">{date_from_str}</b> → <b style="color:#0ea5e9">{date_to_str}</b></div>', unsafe_allow_html=True)
with col_h2:
    refresh = st.button("🔄 Refresh Data", use_container_width=True)

st.markdown("---")

# Gate – require connection
if not st.session_state.token:
    st.markdown("""
    <div class="info-box">
        👈 Enter your <b>DealPOS subdomain</b>, <b>Client ID</b>, and <b>Client Secret</b> in the sidebar, then click <b>Connect</b> to load your reports.
    </div>
    """, unsafe_allow_html=True)

    st.markdown("### 🗂️ Dashboard Preview")
    tab1, tab2, tab3 = st.tabs(["📈 Sales Report", "📦 Inventory Check", "🚚 Logistics"])

    with tab1:
        st.info("Connect to DealPOS to see Sales data — invoices, revenue trends, top products, payment methods.")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Revenue", "—")
        c2.metric("Total Invoices", "—")
        c3.metric("Avg Order Value", "—")
        c4.metric("Unique Customers", "—")

    with tab2:
        st.info("Connect to see Inventory levels, low stock alerts, and stock by outlet.")

    with tab3:
        st.info("Connect to see Outbound and Inbound logistics status, shipment counts, and delivery tracking.")

    st.stop()


# ─────────────────────────────────────────────────────────────
# TABS
# ─────────────────────────────────────────────────────────────
tab1, tab2, tab3 = st.tabs(["📈  Sales Report", "📦  Inventory Check", "🚚  Logistics"])


# ══════════════════════════════════════════════════════════════
# TAB 1 — SALES REPORT
# ══════════════════════════════════════════════════════════════
with tab1:
    st.markdown("### Sales Overview")

    params = {
        "DateFrom": date_from_str,
        "DateTo": date_to_str,
    }
    if outlet_code:
        params["OutletCode"] = outlet_code

    with st.spinner("Loading sales data..."):
        # 1. Invoice list (main sales data)
        inv_data = safe_get("/Invoice/MultipleOutlet/WithTotalCount", params, "invoices")
        # 2. Daily sales
        daily_data = safe_get("/Report/DailySales", {**params, "Month": date_from.strftime("%Y-%m")}, "daily sales")
        # 3. Product sold
        prod_data = safe_get("/Report/ProductSold", params, "product sold")
        # 4. Payment method breakdown (from report)
        report_data = safe_get("/Report", params, "report")

    # ── KPI CARDS ──
    inv_df = to_df(inv_data)
    total_revenue = 0
    total_invoices = 0
    avg_order = 0
    unique_customers = 0

    if not inv_df.empty:
        # Detect column names (API may vary)
        rev_col = next((c for c in inv_df.columns if "total" in c.lower() or "amount" in c.lower() or "grandtotal" in c.lower()), None)
        cust_col = next((c for c in inv_df.columns if "customer" in c.lower() or "email" in c.lower()), None)

        total_invoices = len(inv_df)
        if rev_col:
            inv_df[rev_col] = pd.to_numeric(inv_df[rev_col], errors="coerce").fillna(0)
            total_revenue = inv_df[rev_col].sum()
            avg_order = total_revenue / total_invoices if total_invoices else 0
        if cust_col:
            unique_customers = inv_df[cust_col].nunique()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("💰 Total Revenue", fmt_idr(total_revenue) if total_revenue else "—")
    c2.metric("🧾 Total Invoices", f"{total_invoices:,}" if total_invoices else "—")
    c3.metric("📊 Avg Order Value", fmt_idr(avg_order) if avg_order else "—")
    c4.metric("👥 Unique Customers", f"{unique_customers:,}" if unique_customers else "—")

    st.markdown("---")

    col_left, col_right = st.columns([2, 1])

    # ── REVENUE TREND ──
    with col_left:
        st.markdown("#### Revenue Trend")
        daily_df = to_df(daily_data)
        if not daily_df.empty:
            date_col = next((c for c in daily_df.columns if "date" in c.lower() or "day" in c.lower()), None)
            val_col = next((c for c in daily_df.columns if "total" in c.lower() or "amount" in c.lower() or "sales" in c.lower() or "revenue" in c.lower()), None)
            if date_col and val_col:
                daily_df[val_col] = pd.to_numeric(daily_df[val_col], errors="coerce").fillna(0)
                fig = px.area(daily_df, x=date_col, y=val_col,
                    template="plotly_dark",
                    color_discrete_sequence=["#0ea5e9"],
                    labels={val_col: f"Revenue ({currency})", date_col: "Date"})
                fig.update_layout(
                    paper_bgcolor="#1e293b", plot_bgcolor="#1e293b",
                    margin=dict(l=0, r=0, t=10, b=0), height=260,
                    font=dict(family="IBM Plex Mono", color="#94a3b8", size=11),
                    xaxis=dict(gridcolor="#334155"), yaxis=dict(gridcolor="#334155")
                )
                fig.update_traces(line_width=2, fill="tozeroy", fillcolor="rgba(14,165,233,0.15)")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Daily trend data columns not recognized.")
        elif not inv_df.empty:
            # Build trend from invoice list
            date_col = next((c for c in inv_df.columns if "date" in c.lower() or "created" in c.lower()), None)
            rev_col = next((c for c in inv_df.columns if "total" in c.lower() or "amount" in c.lower()), None)
            if date_col and rev_col:
                inv_df[date_col] = pd.to_datetime(inv_df[date_col], errors="coerce")
                inv_df[rev_col] = pd.to_numeric(inv_df[rev_col], errors="coerce").fillna(0)
                trend = inv_df.groupby(inv_df[date_col].dt.date)[rev_col].sum().reset_index()
                trend.columns = ["Date", "Revenue"]
                fig = px.area(trend, x="Date", y="Revenue",
                    template="plotly_dark", color_discrete_sequence=["#0ea5e9"])
                fig.update_layout(
                    paper_bgcolor="#1e293b", plot_bgcolor="#1e293b",
                    margin=dict(l=0, r=0, t=10, b=0), height=260,
                    font=dict(family="IBM Plex Mono", color="#94a3b8", size=11),
                    xaxis=dict(gridcolor="#334155"), yaxis=dict(gridcolor="#334155")
                )
                fig.update_traces(line_width=2, fill="tozeroy", fillcolor="rgba(14,165,233,0.15)")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No trend data available for this period.")
        else:
            st.info("No invoice data available for this period.")

    # ── TOP PRODUCTS ──
    with col_right:
        st.markdown("#### Top Products Sold")
        prod_df = to_df(prod_data)
        if not prod_df.empty:
            name_col = next((c for c in prod_df.columns if "name" in c.lower() or "product" in c.lower()), None)
            qty_col = next((c for c in prod_df.columns if "qty" in c.lower() or "quantity" in c.lower() or "sold" in c.lower()), None)
            if name_col and qty_col:
                prod_df[qty_col] = pd.to_numeric(prod_df[qty_col], errors="coerce").fillna(0)
                top10 = prod_df.nlargest(10, qty_col)[[name_col, qty_col]]
                fig = px.bar(top10, x=qty_col, y=name_col, orientation="h",
                    template="plotly_dark", color_discrete_sequence=["#f59e0b"],
                    labels={qty_col: "Qty Sold", name_col: ""})
                fig.update_layout(
                    paper_bgcolor="#1e293b", plot_bgcolor="#1e293b",
                    margin=dict(l=0, r=0, t=10, b=0), height=260,
                    font=dict(family="IBM Plex Mono", color="#94a3b8", size=10),
                    xaxis=dict(gridcolor="#334155"), yaxis=dict(gridcolor="#334155")
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Product sold columns not recognized.")
        else:
            st.info("No product sold data available.")

    st.markdown("---")

    # ── INVOICE TABLE ──
    st.markdown("#### Invoice Details")
    if not inv_df.empty:
        # Pick relevant columns to display
        display_cols = [c for c in inv_df.columns if any(k in c.lower() for k in
            ["number", "date", "customer", "total", "status", "outlet", "payment", "amount", "grand"])]
        show_df = inv_df[display_cols] if display_cols else inv_df
        show_df = show_df.head(200)

        # Search filter
        search = st.text_input("🔍 Search invoices", placeholder="Invoice number, customer name...")
        if search:
            mask = show_df.astype(str).apply(lambda col: col.str.contains(search, case=False)).any(axis=1)
            show_df = show_df[mask]

        st.dataframe(show_df, use_container_width=True, height=350)
        st.caption(f"Showing {len(show_df)} records")

        # Download
        csv = inv_df.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Download Invoices CSV", csv, "dealpos_invoices.csv", "text/csv")
    else:
        st.info("No invoice records found for this period.")

    # ── PAYMENT METHOD BREAKDOWN ──
    st.markdown("#### Payment Method Breakdown")
    pay_data = safe_get("/Payment/Invoice", params, "payments")
    pay_df = to_df(pay_data)
    if not pay_df.empty:
        method_col = next((c for c in pay_df.columns if "method" in c.lower() or "payment" in c.lower() and "method" in c.lower()), None)
        amt_col = next((c for c in pay_df.columns if "amount" in c.lower() or "total" in c.lower()), None)
        if method_col and amt_col:
            pay_df[amt_col] = pd.to_numeric(pay_df[amt_col], errors="coerce").fillna(0)
            grouped = pay_df.groupby(method_col)[amt_col].sum().reset_index()
            grouped.columns = ["Payment Method", "Total Amount"]
            col_pie, col_tbl = st.columns([1, 1])
            with col_pie:
                fig = px.pie(grouped, names="Payment Method", values="Total Amount",
                    template="plotly_dark",
                    color_discrete_sequence=px.colors.sequential.Blues_r)
                fig.update_layout(paper_bgcolor="#1e293b", margin=dict(l=0, r=0, t=10, b=0), height=250,
                    font=dict(family="IBM Plex Mono", color="#94a3b8", size=11))
                st.plotly_chart(fig, use_container_width=True)
            with col_tbl:
                grouped["Total Amount"] = grouped["Total Amount"].apply(fmt_idr)
                st.dataframe(grouped, use_container_width=True, hide_index=True)
        else:
            st.info("Payment method columns not recognized.")
    else:
        st.info("No payment data available.")


# ══════════════════════════════════════════════════════════════
# TAB 2 — INVENTORY CHECK
# ══════════════════════════════════════════════════════════════
with tab2:
    st.markdown("### Inventory Overview")

    inv_params = {}
    if outlet_code:
        inv_params["OutletCode"] = outlet_code

    with st.spinner("Loading inventory data..."):
        stock_data = safe_get("/Inventory", inv_params, "inventory")
        stock_tc = safe_get("/Inventory/WithTotalCount", inv_params, "inventory count")

    stock_df = to_df(stock_data)

    # KPIs
    total_skus = 0
    low_stock_count = 0
    out_of_stock = 0
    total_value = 0

    qty_col = None
    price_col = None

    if not stock_df.empty:
        qty_col = next((c for c in stock_df.columns if "onhand" in c.lower() or "qty" in c.lower() or "quantity" in c.lower() or "stock" in c.lower()), None)
        price_col = next((c for c in stock_df.columns if "price" in c.lower() or "cost" in c.lower()), None)

        total_skus = len(stock_df)
        if qty_col:
            stock_df[qty_col] = pd.to_numeric(stock_df[qty_col], errors="coerce").fillna(0)
            low_stock_count = int((stock_df[qty_col] > 0) & (stock_df[qty_col] <= low_stock_threshold)).sum()
            out_of_stock = int((stock_df[qty_col] <= 0).sum())
        if qty_col and price_col:
            stock_df[price_col] = pd.to_numeric(stock_df[price_col], errors="coerce").fillna(0)
            total_value = (stock_df[qty_col] * stock_df[price_col]).sum()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("📦 Total SKUs", f"{total_skus:,}" if total_skus else "—")
    c2.metric(f"⚠️ Low Stock (≤{low_stock_threshold})", f"{low_stock_count:,}" if stock_df is not None and not stock_df.empty else "—")
    c3.metric("❌ Out of Stock", f"{out_of_stock:,}" if stock_df is not None and not stock_df.empty else "—")
    c4.metric("💎 Est. Stock Value", fmt_idr(total_value) if total_value else "—")

    st.markdown("---")

    if not stock_df.empty and qty_col:
        col_left, col_right = st.columns([1, 1])

        # ── STOCK DISTRIBUTION ──
        with col_left:
            st.markdown("#### Stock Level Distribution")
            bins = [0, 0.01, low_stock_threshold, low_stock_threshold * 5, float("inf")]
            labels_b = ["Out of Stock", f"Low (1–{low_stock_threshold})", f"Medium ({low_stock_threshold+1}–{low_stock_threshold*5})", "High"]
            stock_df["Stock Category"] = pd.cut(stock_df[qty_col], bins=bins, labels=labels_b, include_lowest=True)
            dist = stock_df["Stock Category"].value_counts().reset_index()
            dist.columns = ["Category", "Count"]
            colors = {"Out of Stock": "#ef4444", f"Low (1–{low_stock_threshold})": "#f59e0b",
                      f"Medium ({low_stock_threshold+1}–{low_stock_threshold*5})": "#0ea5e9", "High": "#10b981"}
            fig = px.pie(dist, names="Category", values="Count",
                template="plotly_dark",
                color="Category",
                color_discrete_map=colors)
            fig.update_layout(paper_bgcolor="#1e293b", margin=dict(l=0, r=0, t=10, b=0), height=280,
                font=dict(family="IBM Plex Mono", color="#94a3b8", size=11))
            st.plotly_chart(fig, use_container_width=True)

        # ── TOP 10 BY STOCK ──
        with col_right:
            st.markdown("#### Top 10 Items by Stock On-Hand")
            name_col = next((c for c in stock_df.columns if "name" in c.lower() or "product" in c.lower() or "variant" in c.lower()), None)
            if name_col:
                top_stock = stock_df.nlargest(10, qty_col)[[name_col, qty_col]]
                fig = px.bar(top_stock, x=qty_col, y=name_col, orientation="h",
                    template="plotly_dark", color_discrete_sequence=["#10b981"],
                    labels={qty_col: "Qty On-Hand", name_col: ""})
                fig.update_layout(
                    paper_bgcolor="#1e293b", plot_bgcolor="#1e293b",
                    margin=dict(l=0, r=0, t=10, b=0), height=280,
                    font=dict(family="IBM Plex Mono", color="#94a3b8", size=10),
                    xaxis=dict(gridcolor="#334155"), yaxis=dict(gridcolor="#334155")
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Product name column not recognized.")

        st.markdown("---")

        # ── INVENTORY TABLE ──
        st.markdown("#### Full Inventory Table")

        # Filters
        col_f1, col_f2, col_f3 = st.columns(3)
        with col_f1:
            search_inv = st.text_input("🔍 Search product/SKU", placeholder="Code or name...", key="inv_search")
        with col_f2:
            stock_filter = st.selectbox("Filter by stock level",
                ["All", "Out of Stock", f"Low (≤{low_stock_threshold})", "In Stock"])
        with col_f3:
            if "Stock Category" in stock_df.columns:
                outlet_col = next((c for c in stock_df.columns if "outlet" in c.lower()), None)
                if outlet_col:
                    outlets = ["All"] + sorted(stock_df[outlet_col].dropna().unique().tolist())
                    outlet_filter = st.selectbox("Filter by Outlet", outlets, key="inv_outlet")
                else:
                    outlet_filter = "All"
            else:
                outlet_filter = "All"

        display_inv = stock_df.copy()
        if search_inv:
            mask = display_inv.astype(str).apply(lambda col: col.str.contains(search_inv, case=False)).any(axis=1)
            display_inv = display_inv[mask]
        if stock_filter == "Out of Stock":
            display_inv = display_inv[display_inv[qty_col] <= 0]
        elif stock_filter == f"Low (≤{low_stock_threshold})":
            display_inv = display_inv[(display_inv[qty_col] > 0) & (display_inv[qty_col] <= low_stock_threshold)]
        elif stock_filter == "In Stock":
            display_inv = display_inv[display_inv[qty_col] > 0]
        if outlet_filter != "All" and outlet_col in display_inv.columns:
            display_inv = display_inv[display_inv[outlet_col] == outlet_filter]

        # Highlight low stock
        def highlight_stock(row):
            try:
                val = float(row[qty_col])
                if val <= 0:
                    return [f"background-color: rgba(239,68,68,0.15)"] * len(row)
                elif val <= low_stock_threshold:
                    return [f"background-color: rgba(245,158,11,0.15)"] * len(row)
            except Exception:
                pass
            return [""] * len(row)

        styled = display_inv.head(300).style.apply(highlight_stock, axis=1)
        st.dataframe(styled, use_container_width=True, height=400)
        st.caption(f"Showing {len(display_inv)} records | 🔴 Out of Stock  🟡 Low Stock")

        csv_inv = stock_df.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Download Inventory CSV", csv_inv, "dealpos_inventory.csv", "text/csv")

    else:
        st.info("No inventory data available. Check your outlet code or API permissions.")

    # ── INVENTORY LOG ──
    st.markdown("---")
    st.markdown("#### Recent Inventory Log")
    with st.expander("View Inventory Movement Log"):
        code_input = st.text_input("Variant Code to check log", placeholder="e.g. VAR001")
        if code_input:
            with st.spinner("Loading log..."):
                log_data = safe_get("/Inventory/Log", {"Code": code_input}, "inventory log")
            log_df = to_df(log_data)
            if not log_df.empty:
                st.dataframe(log_df, use_container_width=True, height=300)
            else:
                st.info("No log data found for this code.")


# ══════════════════════════════════════════════════════════════
# TAB 3 — LOGISTICS
# ══════════════════════════════════════════════════════════════
with tab3:
    st.markdown("### Logistics Overview")

    log_params = {
        "DateFrom": date_from_str,
        "DateTo": date_to_str,
    }
    if outlet_code:
        log_params["OutletCode"] = outlet_code

    with st.spinner("Loading logistics data..."):
        ob_data = safe_get("/OutboundLogistic/WithTotalCount", log_params, "outbound logistics")
        ib_data = safe_get("/InboundLogistic/WithTotalCount", log_params, "inbound logistics")
        to_data = safe_get("/TransferOrder/WithTotalCount", log_params, "transfer orders")

    ob_df = to_df(ob_data)
    ib_df = to_df(ib_data)
    to_df_ = to_df(to_data)

    # KPIs
    total_outbound = len(ob_df) if not ob_df.empty else 0
    total_inbound = len(ib_df) if not ib_df.empty else 0
    total_transfer = len(to_df_) if not to_df_.empty else 0

    # Try total count from envelope
    if isinstance(ob_data, dict):
        total_outbound = ob_data.get("TotalCount") or ob_data.get("totalCount") or total_outbound
    if isinstance(ib_data, dict):
        total_inbound = ib_data.get("TotalCount") or ib_data.get("totalCount") or total_inbound
    if isinstance(to_data, dict):
        total_transfer = to_data.get("TotalCount") or to_data.get("totalCount") or total_transfer

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("📤 Outbound Shipments", f"{total_outbound:,}")
    c2.metric("📥 Inbound Receipts", f"{total_inbound:,}")
    c3.metric("🔄 Transfer Orders", f"{total_transfer:,}")
    total_movements = (int(total_outbound) + int(total_inbound) + int(total_transfer))
    c4.metric("🔢 Total Movements", f"{total_movements:,}")

    st.markdown("---")

    # ── LOGISTICS VOLUME CHART ──
    col_chart, col_status = st.columns([2, 1])

    with col_chart:
        st.markdown("#### Logistics Volume by Type")
        vol_data = {
            "Type": ["Outbound", "Inbound", "Transfer"],
            "Count": [int(total_outbound), int(total_inbound), int(total_transfer)]
        }
        vol_df = pd.DataFrame(vol_data)
        fig = px.bar(vol_df, x="Type", y="Count",
            template="plotly_dark",
            color="Type",
            color_discrete_map={"Outbound": "#0ea5e9", "Inbound": "#10b981", "Transfer": "#f59e0b"},
            text="Count")
        fig.update_traces(textposition="outside", marker_line_width=0)
        fig.update_layout(
            paper_bgcolor="#1e293b", plot_bgcolor="#1e293b",
            margin=dict(l=0, r=0, t=10, b=0), height=280,
            font=dict(family="IBM Plex Mono", color="#94a3b8", size=12),
            xaxis=dict(gridcolor="#334155", title=""),
            yaxis=dict(gridcolor="#334155", title="Count"),
            showlegend=False
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_status:
        st.markdown("#### Status Summary")

        def count_status(df):
            if df.empty:
                return {}
            status_col = next((c for c in df.columns if "status" in c.lower() or "state" in c.lower()), None)
            if status_col:
                return df[status_col].value_counts().to_dict()
            return {}

        ob_status = count_status(ob_df)
        ib_status = count_status(ib_df)
        to_status = count_status(to_df_)

        all_statuses = {}
        for k, v in ob_status.items():
            all_statuses[f"OB: {k}"] = v
        for k, v in ib_status.items():
            all_statuses[f"IB: {k}"] = v
        for k, v in to_status.items():
            all_statuses[f"TO: {k}"] = v

        if all_statuses:
            status_df = pd.DataFrame(list(all_statuses.items()), columns=["Status", "Count"])
            fig = px.pie(status_df, names="Status", values="Count",
                template="plotly_dark",
                color_discrete_sequence=px.colors.sequential.Blues_r)
            fig.update_layout(paper_bgcolor="#1e293b", margin=dict(l=0, r=0, t=10, b=0), height=280,
                font=dict(family="IBM Plex Mono", color="#94a3b8", size=10))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No status data to display.")

    st.markdown("---")

    # ── OUTBOUND ──
    st.markdown("#### 📤 Outbound Logistics")
    if not ob_df.empty:
        ob_search = st.text_input("🔍 Search outbound", placeholder="Number, tracking, customer...", key="ob_search")
        disp_ob = ob_df.copy()
        if ob_search:
            mask = disp_ob.astype(str).apply(lambda col: col.str.contains(ob_search, case=False)).any(axis=1)
            disp_ob = disp_ob[mask]

        # Trend by date
        date_col_ob = next((c for c in ob_df.columns if "date" in c.lower() or "created" in c.lower()), None)
        if date_col_ob:
            ob_df[date_col_ob] = pd.to_datetime(ob_df[date_col_ob], errors="coerce")
            ob_trend = ob_df.groupby(ob_df[date_col_ob].dt.date).size().reset_index(name="Shipments")
            ob_trend.columns = ["Date", "Shipments"]
            fig = px.line(ob_trend, x="Date", y="Shipments",
                template="plotly_dark", color_discrete_sequence=["#0ea5e9"],
                title="Outbound Shipments per Day")
            fig.update_layout(paper_bgcolor="#1e293b", plot_bgcolor="#1e293b",
                margin=dict(l=0, r=0, t=40, b=0), height=200,
                font=dict(family="IBM Plex Mono", color="#94a3b8", size=10),
                xaxis=dict(gridcolor="#334155"), yaxis=dict(gridcolor="#334155"))
            st.plotly_chart(fig, use_container_width=True)

        st.dataframe(disp_ob.head(200), use_container_width=True, height=300)
        csv_ob = ob_df.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Download Outbound CSV", csv_ob, "dealpos_outbound.csv", "text/csv")
    else:
        st.info("No outbound logistics records found.")

    st.markdown("---")

    # ── INBOUND ──
    st.markdown("#### 📥 Inbound Logistics")
    if not ib_df.empty:
        ib_search = st.text_input("🔍 Search inbound", placeholder="Number, supplier...", key="ib_search")
        disp_ib = ib_df.copy()
        if ib_search:
            mask = disp_ib.astype(str).apply(lambda col: col.str.contains(ib_search, case=False)).any(axis=1)
            disp_ib = disp_ib[mask]
        st.dataframe(disp_ib.head(200), use_container_width=True, height=300)
        csv_ib = ib_df.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Download Inbound CSV", csv_ib, "dealpos_inbound.csv", "text/csv")
    else:
        st.info("No inbound logistics records found.")

    st.markdown("---")

    # ── TRANSFER ORDERS ──
    st.markdown("#### 🔄 Transfer Orders")
    if not to_df_.empty:
        to_search = st.text_input("🔍 Search transfer orders", placeholder="Number, outlet...", key="to_search")
        disp_to = to_df_.copy()
        if to_search:
            mask = disp_to.astype(str).apply(lambda col: col.str.contains(to_search, case=False)).any(axis=1)
            disp_to = disp_to[mask]

        # Status filter for transfer
        status_col_to = next((c for c in to_df_.columns if "status" in c.lower() or "state" in c.lower()), None)
        if status_col_to:
            statuses = ["All"] + sorted(to_df_[status_col_to].dropna().unique().tolist())
            sel_status = st.selectbox("Filter Status", statuses, key="to_status")
            if sel_status != "All":
                disp_to = disp_to[disp_to[status_col_to] == sel_status]

        st.dataframe(disp_to.head(200), use_container_width=True, height=300)
        csv_to = to_df_.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Download Transfer Orders CSV", csv_to, "dealpos_transfer_orders.csv", "text/csv")
    else:
        st.info("No transfer order records found.")

    # ── TRACK BY NUMBER ──
    st.markdown("---")
    st.markdown("#### 🔎 Track Shipment by Number")
    col_t1, col_t2 = st.columns([2, 1])
    with col_t1:
        track_num = st.text_input("Enter Order / Logistic Number", placeholder="e.g. OD-2024-001")
    with col_t2:
        track_type = st.selectbox("Type", ["Outbound", "Inbound", "Transfer Order"])

    if st.button("🔍 Track", use_container_width=False) and track_num:
        with st.spinner("Fetching shipment details..."):
            if track_type == "Outbound":
                result = safe_get("/OutboundLogistic/Number", {"Number": track_num}, "outbound detail")
                if not result:
                    result = safe_get("/OutboundLogistic/byOrderNumber", {"OrderNumber": track_num}, "outbound by order")
            elif track_type == "Inbound":
                result = safe_get("/InboundLogistic/Number", {"Number": track_num}, "inbound detail")
                if not result:
                    result = safe_get("/InboundLogistic/byOrderNumber", {"OrderNumber": track_num}, "inbound by order")
            else:
                result = safe_get("/TransferOrder/Detail", {"Number": track_num}, "transfer detail")

        if result:
            st.success("✅ Shipment found!")
            if isinstance(result, dict):
                for k, v in result.items():
                    col_k, col_v = st.columns([1, 2])
                    col_k.markdown(f'<span style="color:#64748b;font-size:12px;">{k}</span>', unsafe_allow_html=True)
                    col_v.markdown(f'<span style="color:#e2e8f0;font-size:13px;">{v}</span>', unsafe_allow_html=True)
            else:
                st.json(result)
        else:
            st.warning(f"No shipment found for number: **{track_num}**")


# ─────────────────────────────────────────────────────────────
# FOOTER
# ─────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown(
    '<div style="text-align:center;color:#334155;font-size:11px;font-family:IBM Plex Mono;">DealPOS Report Dashboard • Built with Streamlit • API v3.1.0</div>',
    unsafe_allow_html=True
)
