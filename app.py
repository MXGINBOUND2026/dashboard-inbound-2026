import datetime
import gc
import io
import os
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

st.set_page_config(
    page_title="Database Inbound 2026",
    page_icon="🚚",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =========================================================
# CSS STYLING (PURE CSS INFOGRAPHIC COMPONENTS - 0 RAM)
# =========================================================
st.markdown(
    """
    <style>
    /* Header Banner */
    .hero-header {
        background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%);
        border: 1px solid #334155;
        border-left: 5px solid #38bdf8;
        border-radius: 12px;
        padding: 16px 22px;
        margin-bottom: 14px;
        display: flex;
        justify-content: space-between;
        align-items: center;
        flex-wrap: wrap;
        gap: 10px;
    }
    .hero-title {
        font-size: 24px;
        font-weight: 800;
        color: #f8fafc;
        margin: 0;
        letter-spacing: -0.3px;
    }
    .hero-subtitle {
        font-size: 13px;
        color: #94a3b8;
        margin-top: 2px;
    }
    .update-badge {
        background-color: rgba(56, 189, 248, 0.12);
        border: 1px solid rgba(56, 189, 248, 0.35);
        color: #38bdf8;
        padding: 6px 12px;
        border-radius: 20px;
        font-size: 12px;
        font-weight: 600;
    }

    /* Marquee Ticker */
    @keyframes marquee {
        0% { transform: translateX(100%); }
        100% { transform: translateX(-100%); }
    }
    .marquee-container-tab1 {
        width: 100%;
        overflow: hidden;
        white-space: nowrap;
        background: #0f172a;
        border: 1px solid #1e293b;
        padding: 8px 0;
        border-radius: 8px;
        box-sizing: border-box;
        margin-bottom: 16px;
    }
    .marquee-text-tab1 {
        display: inline-block;
        padding-left: 35%;
        animation: marquee 45s linear infinite;
        color: #fbbf24;
        font-weight: 600;
        font-size: 13px;
    }

    /* Infographic KPI Cards */
    .kpi-card {
        background: linear-gradient(180deg, #1e293b 0%, #0f172a 100%);
        border: 1px solid #334155;
        border-radius: 12px;
        padding: 14px 16px;
        margin-bottom: 8px;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.25);
    }
    .kpi-top {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 6px;
        gap: 6px;
    }
    .kpi-label {
        font-size: 12px;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.4px;
        color: #94a3b8;
    }
    .kpi-pill {
        font-size: 10px;
        font-weight: 700;
        padding: 2px 8px;
        border-radius: 12px;
        white-space: nowrap;
    }
    .kpi-value {
        font-size: 28px;
        font-weight: 800;
        color: #f8fafc;
        line-height: 1.15;
        margin: 4px 0 8px 0;
    }
    .kpi-sub {
        font-size: 12px;
        color: #cbd5e1;
        margin-bottom: 8px;
    }
    .progress-track {
        width: 100%;
        height: 6px;
        background-color: #334155;
        border-radius: 999px;
        overflow: hidden;
    }
    .progress-fill {
        height: 100%;
        border-radius: 999px;
    }

    /* Section Title Banner */
    .section-banner {
        background-color: #1e293b;
        border-left: 4px solid #38bdf8;
        padding: 8px 14px;
        border-radius: 6px;
        font-size: 15px;
        font-weight: 700;
        color: #f1f5f9;
        margin: 12px 0;
    }

    /* SLA Legend Bar */
    .sla-legend {
        display: flex;
        flex-wrap: wrap;
        gap: 14px;
        font-size: 12px;
        color: #cbd5e1;
        background: #0f172a;
        border: 1px solid #334155;
        padding: 6px 12px;
        border-radius: 8px;
        margin-bottom: 8px;
    }
    .dot-green { display:inline-block; width:10px; height:10px; background:#28a745; border-radius:2px; margin-right:5px; }
    .dot-yellow { display:inline-block; width:10px; height:10px; background:#ffc107; border-radius:2px; margin-right:5px; }
    .dot-red { display:inline-block; width:10px; height:10px; background:#dc3545; border-radius:2px; margin-right:5px; }
    .dot-zero { display:inline-block; width:10px; height:10px; background:#1e293b; border:1px solid #475569; border-radius:2px; margin-right:5px; }
    </style>
    """,
    unsafe_allow_html=True,
)

TARGET_GDRIVE_FOLDER_ID = "1qLpozZT-UX2Wm_6F6FzCIjESUkEgbMaP"
OUTPUT_PARQUET_NAME = "data_inbound_2026.parquet"
LOCAL_PARQUET_PATH = os.path.join(os.getcwd(), OUTPUT_PARQUET_NAME)

AGING_ORDER = ["H+0", "H+1", "H+2", "H+3", "H+4", "H+5", "H+6", "H+7", ">H+7"]
AGING_COLOR_MAP = {
    "H+0": "#28a745",   # Hijau (H+0)
    "H+1": "#ffc107",   # Kuning (H+1)
    "H+2": "#ffc107",   # Kuning (H+2)
    "H+3": "#dc3545",   # Merah (H+3 s/d >H+7)
    "H+4": "#dc3545",
    "H+5": "#dc3545",
    "H+6": "#dc3545",
    "H+7": "#dc3545",
    ">H+7": "#dc3545",
}
EXEC_PASSWORD = (
    st.secrets.get("SUMMARY_EXEC_PASS", "MXG125")
    if hasattr(st, "secrets")
    else "MXG125"
)


# =========================================================
# ENGINE DUCKDB TERPUSAT & HEMAT RAM
# =========================================================
@st.cache_resource
def get_duckdb_con():
    con = duckdb.connect(database=":memory:", read_only=False)
    con.execute("SET memory_limit='512MB';")
    con.execute("SET threads=2;")
    return con


def ensure_view():
    con = get_duckdb_con()
    con.execute(
        f"CREATE OR REPLACE VIEW v_inbound AS SELECT * FROM read_parquet('{LOCAL_PARQUET_PATH}')"
    )
    return con


@st.cache_data(ttl=3600, show_spinner=False)
def run_query(sql_query):
    con = ensure_view()
    return con.execute(sql_query).fetchdf()


def get_drive_service():
    SCOPES = ["https://www.googleapis.com/auth/drive"]
    creds_dict = None
    if hasattr(st, "secrets") and "gcp_service_account" in st.secrets:
        creds_dict = dict(st.secrets["gcp_service_account"])
    if creds_dict and "private_key" in creds_dict:
        pk = str(creds_dict["private_key"]).replace("\\n", "\n").strip()
        creds_dict["private_key"] = pk
    return build(
        "drive",
        "v3",
        credentials=service_account.Credentials.from_service_account_info(
            creds_dict, scopes=SCOPES
        ),
    )


def download_file_from_drive(service, folder_id, file_name, local_path):
    try:
        query = (
            f"'{folder_id}' in parents and name = '{file_name}' and trashed = false"
        )
        results = service.files().list(q=query, fields="files(id, name)").execute()
        items = results.get("files", [])
        if not items:
            return False
        file_id = items[0]["id"]
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        with open(local_path, "wb") as f:
            f.write(fh.getvalue())
        return True
    except Exception:
        return False


@st.cache_resource
def sync_parquet_from_gdrive():
    with st.spinner("🔄 Mengunduh file database terbaru..."):
        try:
            service = get_drive_service()
            return download_file_from_drive(
                service,
                TARGET_GDRIVE_FOLDER_ID,
                OUTPUT_PARQUET_NAME,
                LOCAL_PARQUET_PATH,
            )
        except Exception as e:
            st.error(f"Kesalahan GDrive: {e}")
            return False


# Helper HTML Infographic Card
def render_kpi_card(
    title,
    value_str,
    sub_str,
    accent_color="#38bdf8",
    pill_text=None,
    progress_pct=None,
):
    pct_clamped = (
        max(0.0, min(100.0, float(progress_pct)))
        if progress_pct is not None
        else 0.0
    )
    pill_html = (
        f'<span class="kpi-pill" style="background:{accent_color}22; color:{accent_color}; border:1px solid {accent_color}55;">{pill_text}</span>'
        if pill_text
        else ""
    )
    bar_html = (
        f"""
        <div class="progress-track">
            <div class="progress-fill" style="width:{pct_clamped:.1f}%; background:{accent_color};"></div>
        </div>
        """
        if progress_pct is not None
        else ""
    )
    return f"""
    <div class="kpi-card" style="border-top: 3px solid {accent_color};">
        <div class="kpi-top">
            <span class="kpi-label">{title}</span>
            {pill_html}
        </div>
        <div class="kpi-value">{value_str}</div>
        <div class="kpi-sub">{sub_str}</div>
        {bar_html}
    </div>
    """


# =========================================================
# SIDEBAR CONFIGURATION
# =========================================================
st.sidebar.header("⚙️ Sinkronisasi & Akses")
if st.sidebar.button("🔄 Refresh & Tarik Data Terbaru", use_container_width=True):
    st.cache_resource.clear()
    st.cache_data.clear()
    if os.path.exists(LOCAL_PARQUET_PATH):
        os.remove(LOCAL_PARQUET_PATH)
    gc.collect()
    st.success("Cache dibersihkan! Mengunduh file terbaru...")
    st.rerun()

with st.sidebar:
    st.markdown("---")
    st.subheader("🔐 Keamanan Executive")
    summary_pass = st.text_input(
        "Password Summary Exec:", type="password", key="summary_pass_input"
    )

if not os.path.exists(LOCAL_PARQUET_PATH):
    if not sync_parquet_from_gdrive():
        st.error("File database tidak dapat diunduh. Silakan cek koneksi/GDrive.")
        st.stop()


try:
    st.sidebar.markdown("---")
    st.sidebar.header("🔍 Filter Data Dashboard")

    @st.cache_data(ttl=3600, show_spinner=False)
    def get_filter_options():
        con = ensure_view()
        locs = (
            con.execute(
                "SELECT DISTINCT LAST_LOCATION FROM v_inbound WHERE LAST_LOCATION IS NOT NULL ORDER BY 1"
            )
            .fetchdf()["LAST_LOCATION"]
            .tolist()
        )
        cab = (
            con.execute(
                "SELECT DISTINCT Cabang FROM v_inbound WHERE Cabang IS NOT NULL ORDER BY 1"
            )
            .fetchdf()["Cabang"]
            .tolist()
        )
        zon = (
            con.execute(
                "SELECT DISTINCT Zona FROM v_inbound WHERE Zona IS NOT NULL ORDER BY 1"
            )
            .fetchdf()["Zona"]
            .tolist()
        )
        kec = (
            con.execute(
                "SELECT DISTINCT Kecamatan FROM v_inbound WHERE Kecamatan IS NOT NULL ORDER BY 1"
            )
            .fetchdf()["Kecamatan"]
            .tolist()
        )
        stat = (
            con.execute(
                "SELECT DISTINCT Status_Shipment FROM v_inbound WHERE Status_Shipment IS NOT NULL ORDER BY 1"
            )
            .fetchdf()["Status_Shipment"]
            .tolist()
        )
        hrs = (
            con.execute(
                "SELECT DISTINCT HRS FROM v_inbound WHERE HRS IS NOT NULL ORDER BY 1"
            )
            .fetchdf()["HRS"]
            .tolist()
        )

        minmax = con.execute(
            """
            SELECT MIN(TRY_STRPTIME(Last_Inbound, '%m/%d/%Y %H:%M')), MAX(TRY_STRPTIME(Last_Inbound, '%m/%d/%Y %H:%M')) 
            FROM v_inbound WHERE Last_Inbound IS NOT NULL AND Last_Inbound != '-'
            """
        ).fetchdf()

        return locs, cab, zon, kec, stat, hrs, minmax

    (
        unique_locations,
        unique_cabang_db,
        unique_zona,
        unique_kecamatan,
        unique_status,
        unique_hrs,
        min_max_date,
    ) = get_filter_options()

    allowed_cabang_list = [
        "Malang",
        "Batu",
        "Blitar",
        "Kepanjen",
        "Bululawang",
        "Turen",
    ]
    unique_cabang = [
        c
        for c in unique_cabang_db
        if any(
            allowed.lower() == str(c).strip().lower()
            for allowed in allowed_cabang_list
        )
    ]

    default_end = (
        min_max_date.iloc[0, 1].date()
        if not min_max_date.empty and pd.notnull(min_max_date.iloc[0, 1])
        else datetime.date.today()
    )
    default_start = default_end - datetime.timedelta(days=30)

    with st.sidebar.form("filter_form"):
        enable_date_filter = st.checkbox(
            "📅 Aktifkan Filter Rentang Tanggal", value=True
        )
        date_range = (
            st.date_input(
                "Pilih Rentang Tanggal:", value=(default_start, default_end)
            )
            if enable_date_filter
            else None
        )

        chosen_loc = st.multiselect("🔍 Filter Last Location:", unique_locations)
        chosen_cabang = st.multiselect("🏢 Filter Cabang:", unique_cabang)
        chosen_zona = st.multiselect("⚡ Filter Zona (QCE):", unique_zona)
        chosen_kecamatan = st.multiselect("📍 Filter Kecamatan:", unique_kecamatan)
        chosen_status = st.multiselect("📦 Filter Shipment Status:", unique_status)
        chosen_hrs = st.multiselect("📋 Filter Status HRS:", unique_hrs)

        submit_filter = st.form_submit_button(
            "🚀 Terapkan Filter", use_container_width=True
        )

    def build_in_clause(column_name, selected_list):
        if not selected_list:
            return None
        formatted_items = [
            f"'{str(item).replace(chr(39), chr(39)*2)}'" for item in selected_list
        ]
        return f"{column_name} IN ({', '.join(formatted_items)})"

    where_clauses_general = []
    if date_range and isinstance(date_range, tuple) and len(date_range) == 2:
        start_d, end_d = date_range
        where_clauses_general.append(
            f"CAST(TRY_STRPTIME(Last_Inbound, '%m/%d/%Y %H:%M') AS DATE) BETWEEN '{start_d}' AND '{end_d}'"
        )

    if chosen_loc:
        where_clauses_general.append(build_in_clause("LAST_LOCATION", chosen_loc))
    if chosen_cabang:
        where_clauses_general.append(build_in_clause("Cabang", chosen_cabang))
    if chosen_zona:
        where_clauses_general.append(build_in_clause("Zona", chosen_zona))
    if chosen_kecamatan:
        where_clauses_general.append(build_in_clause("Kecamatan", chosen_kecamatan))
    if chosen_hrs:
        where_clauses_general.append(build_in_clause("HRS", chosen_hrs))

    where_clauses_full = list(where_clauses_general)
    if chosen_status:
        where_clauses_full.append(build_in_clause("Status_Shipment", chosen_status))

    where_sql = (
        "WHERE " + " AND ".join(where_clauses_full)
        if where_clauses_full
        else "WHERE 1=1"
    )
    where_sql_trend = (
        "WHERE " + " AND ".join(where_clauses_general)
        if where_clauses_general
        else "WHERE 1=1"
    )

    # Fetch Last Update timestamp once for Hero Header
    max_date_df = run_query(
        """
        SELECT MAX(COALESCE(
            TRY_STRPTIME(TGL_TARIK_REPORT, '%m/%d/%Y %H:%M'), 
            TRY_STRPTIME(TGL_TARIK_REPORT, '%m/%d/%Y %H:%M:%S'), 
            TRY_STRPTIME(TGL_TARIK_REPORT, '%d-%b-%Y %H:%M'), 
            TRY_CAST(TGL_TARIK_REPORT AS TIMESTAMP)
        )) as max_dt FROM v_inbound
        """
    )
    max_date = max_date_df["max_dt"][0] if not max_date_df.empty else None
    formatted_date = (
        pd.to_datetime(max_date).strftime("%d %b %Y %H:%M")
        if pd.notnull(max_date)
        else "Waktu pembaruan tidak tersedia"
    )

    st.markdown(
        f"""
        <div class="hero-header">
            <div>
                <div class="hero-title">🚚 Operational Command Center — Inbound 2026</div>
                <div class="hero-subtitle">Monitoring SLA Runsheet, UnInbound, & Kepatuhan HRS Real-Time</div>
            </div>
            <div class="update-badge">🕒 Last Update: {formatted_date}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    tab1, tab2 = st.tabs(["📊 Main Dashboard", "🔒 Summary Exec"])

    # =========================================================
    # TAB 1: MAIN DASHBOARD
    # =========================================================
    with tab1:
        with st.spinner("⏳ Memuat Main Dashboard..."):
            top_kec_df = run_query(
                f"""
                SELECT Kecamatan, COUNT(DISTINCT AWB) as total 
                FROM v_inbound {where_sql} 
                AND Status_Shipment IN ('UnRunsheet', 'UnRunsheet with PRA')
                GROUP BY Kecamatan ORDER BY total DESC LIMIT 5
                """
            )
            top_kecamatan_str = (
                " &nbsp;&nbsp;&bull;&nbsp;&nbsp; ".join(
                    [
                        f"{row.Kecamatan} ({row.total:,} AWB)"
                        for _, row in top_kec_df.iterrows()
                    ]
                )
                or "Tidak ada backlog UnRunsheet"
            )

            st.markdown(
                f"""
                <div class="marquee-container-tab1">
                    <div class="marquee-text-tab1">
                        🔥 TOP 5 BOTTLENECK UNRUNSHEET: &nbsp;&nbsp;{top_kecamatan_str}
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

            if "active_view" not in st.session_state:
                st.session_state.active_view = "UnRunsheet"
            if "active_card_filter" not in st.session_state:
                st.session_state.active_card_filter = None
            if "selected_aging_card" not in st.session_state:
                st.session_state.selected_aging_card = None

            # Single-pass aggregation query (ultra-lightweight RAM)
            metrics_res = run_query(
                f"""
                SELECT 
                    COUNT(DISTINCT AWB) AS total_awb,
                    COUNT(DISTINCT CASE WHEN Status_Shipment = 'UnInbound' THEN AWB END) AS total_uninbound,
                    COUNT(DISTINCT CASE WHEN Status_Shipment IN ('UnRunsheet', 'UnRunsheet with PRA') THEN AWB END) AS total_unrunsheet,
                    COUNT(DISTINCT CASE WHEN Status_Shipment = 'UnRunsheet with PRA' THEN AWB END) AS total_unrunsheet_pra,
                    COUNT(DISTINCT CASE WHEN Status_Shipment IN ('SUCCESS DELIVERY', 'SUCCESS RETURN SHIPPER') THEN AWB END) AS total_success,
                    COUNT(DISTINCT CASE WHEN Status_Shipment = 'SUCCESS RETURN SHIPPER' THEN AWB END) AS total_return,
                    COUNT(DISTINCT CASE WHEN Status_Shipment = 'UNDELIVERY' THEN AWB END) AS total_undel,
                    COUNT(DISTINCT CASE WHEN Status_Shipment IN ('SUCCESS DELIVERY', 'SUCCESS RETURN SHIPPER') AND (HRS = 'UnHRS' OR HRS = '-' OR HRS IS NULL) AND UPPER(COALESCE(RUNSHEET_NO, '')) LIKE '%MXG%' THEN AWB END) AS total_success_unhrs,
                    COUNT(DISTINCT CASE WHEN Status_Shipment = 'UNDELIVERY' AND (HRS = 'UnHRS' OR HRS = '-' OR HRS IS NULL) AND UPPER(COALESCE(RUNSHEET_NO, '')) LIKE '%MXG%' THEN AWB END) AS total_undel_unhrs
                FROM v_inbound {where_sql}
                """
            )

            r_m = metrics_res.iloc[0]
            total_awb = int(r_m["total_awb"] or 0)
            total_uninbound = int(r_m["total_uninbound"] or 0)
            total_unrunsheet = int(r_m["total_unrunsheet"] or 0)
            total_unrunsheet_pra = int(r_m["total_unrunsheet_pra"] or 0)
            total_success = int(r_m["total_success"] or 0)
            total_return = int(r_m["total_return"] or 0)
            total_undel = int(r_m["total_undel"] or 0)
            total_success_unhrs = int(r_m["total_success_unhrs"] or 0)
            total_undel_unhrs = int(r_m["total_undel_unhrs"] or 0)

            success_rate = (total_success / total_awb * 100.0) if total_awb > 0 else 0.0
            return_rate = (total_return / total_awb * 100.0) if total_awb > 0 else 0.0
            unrunsheet_rate = (
                (total_unrunsheet / total_awb * 100.0) if total_awb > 0 else 0.0
            )
            uninbound_rate = (
                (total_uninbound / total_awb * 100.0) if total_awb > 0 else 0.0
            )
            pra_share = (
                (total_unrunsheet_pra / total_unrunsheet * 100.0)
                if total_unrunsheet > 0
                else 0.0
            )

            # Row 1: 4 Primary Infographic KPI Cards
            c1, c2, c3, c4 = st.columns(4)
            with c1:
                st.markdown(
                    render_kpi_card(
                        "📦 Total Inbound AWB",
                        f"{total_awb:,}",
                        f"Success: <b>{success_rate:.1f}%</b> &nbsp;|&nbsp; Return: <b>{return_rate:.1f}%</b>",
                        accent_color="#28a745",
                        pill_text=f"SR {success_rate:.1f}%",
                        progress_pct=success_rate,
                    ),
                    unsafe_allow_html=True,
                )
            with c2:
                st.markdown(
                    render_kpi_card(
                        "⚠️ UnInbound Backlog",
                        f"{total_uninbound:,}",
                        f"Porsi dari Total AWB: <b>{uninbound_rate:.2f}%</b>",
                        accent_color="#ffc107" if total_uninbound > 0 else "#28a745",
                        pill_text="PENDING INBOUND",
                        progress_pct=min(uninbound_rate * 5, 100),
                    ),
                    unsafe_allow_html=True,
                )
                if st.button(
                    "🔍 Drilldown UnInbound",
                    key="btn_uninbound",
                    use_container_width=True,
                ):
                    st.session_state.active_view = "UnInbound"
                    st.session_state.active_card_filter = "UnInbound"
                    st.rerun()
            with c3:
                st.markdown(
                    render_kpi_card(
                        "🚨 Total UnRunsheet",
                        f"{total_unrunsheet:,}",
                        f"Rasio Backlog: <b>{unrunsheet_rate:.2f}%</b> dari Total AWB",
                        accent_color="#dc3545" if total_unrunsheet > 0 else "#28a745",
                        pill_text="ACTION NEEDED",
                        progress_pct=min(unrunsheet_rate * 4, 100),
                    ),
                    unsafe_allow_html=True,
                )
                if st.button(
                    "🔍 Drilldown UnRunsheet",
                    key="btn_unrunsheet",
                    use_container_width=True,
                ):
                    st.session_state.active_view = "UnRunsheet"
                    st.session_state.active_card_filter = "Total UnRunsheet"
                    st.rerun()
            with c4:
                st.markdown(
                    render_kpi_card(
                        "🚚 UnRunsheet with PRA",
                        f"{total_unrunsheet_pra:,}",
                        f"Kontribusi: <b>{pra_share:.1f}%</b> dari Total UnRunsheet",
                        accent_color="#38bdf8",
                        pill_text="PRA STATUS",
                        progress_pct=pra_share,
                    ),
                    unsafe_allow_html=True,
                )
                if st.button(
                    "🔍 Drilldown UnRunsheet PRA",
                    key="btn_unrunsheet_pra",
                    use_container_width=True,
                ):
                    st.session_state.active_view = "UnRunsheet PRA"
                    st.session_state.active_card_filter = "UnRunsheet with PRA"
                    st.rerun()

            # Row 2: UnHRS Audit Cards
            col_m1, col_m2 = st.columns(2)
            with col_m1:
                st.markdown(
                    render_kpi_card(
                        "⚠️ Audit POD: Success UnHRS (MXG)",
                        f"{total_success_unhrs:,} AWB",
                        "Shipment Success/Return yang belum ter-closing HRS",
                        accent_color="#ffc107",
                        pill_text="UNCLOSED HRS",
                    ),
                    unsafe_allow_html=True,
                )
                if st.button(
                    "📋 Buka Detail Success UnHRS",
                    key="btn_success_unhrs",
                    use_container_width=True,
                ):
                    st.session_state.active_card_filter = "Success UnHRS"
                    st.rerun()
            with col_m2:
                st.markdown(
                    render_kpi_card(
                        "❌ Audit POD: Undel UnHRS (MXG)",
                        f"{total_undel_unhrs:,} AWB",
                        "Shipment Undelivery yang belum ter-closing HRS",
                        accent_color="#dc3545",
                        pill_text="UNCLOSED UNDEL",
                    ),
                    unsafe_allow_html=True,
                )
                if st.button(
                    "📋 Buka Detail Undel UnHRS",
                    key="btn_undel_unhrs",
                    use_container_width=True,
                ):
                    st.session_state.active_card_filter = "Undel UnHRS"
                    st.rerun()

            # Active Card Drilldown (Only queries DuckDB when explicitly triggered)
            if st.session_state.active_card_filter:
                card_title = st.session_state.active_card_filter
                with st.expander(
                    f"📋 Detail AWB — Filter Aktif: [{card_title}]", expanded=True
                ):
                    if card_title == "Success UnHRS":
                        card_query_filter = "Status_Shipment IN ('SUCCESS DELIVERY', 'SUCCESS RETURN SHIPPER') AND (HRS = 'UnHRS' OR HRS = '-' OR HRS IS NULL) AND UPPER(COALESCE(RUNSHEET_NO, '')) LIKE '%MXG%'"
                    elif card_title == "Undel UnHRS":
                        card_query_filter = "Status_Shipment = 'UNDELIVERY' AND (HRS = 'UnHRS' OR HRS = '-' OR HRS IS NULL) AND UPPER(COALESCE(RUNSHEET_NO, '')) LIKE '%MXG%'"
                    elif card_title == "Total UnRunsheet":
                        card_query_filter = (
                            "Status_Shipment IN ('UnRunsheet', 'UnRunsheet with PRA')"
                        )
                    elif card_title == "UnRunsheet with PRA":
                        card_query_filter = "Status_Shipment = 'UnRunsheet with PRA'"
                    elif card_title == "UnInbound":
                        card_query_filter = "Status_Shipment = 'UnInbound'"
                    else:
                        card_query_filter = "1=1"

                    df_card_detail_full = run_query(
                        f"""
                        SELECT AWB, Last_Inbound, TGL_UPDATE_STATUS_POD, RUNSHEET_NO, payment_type, age_days AS "Aging (Hari)", LAST_LOCATION, Cabang, Zona, Kecamatan, SERVICE, Status_Shipment, HRS, RUNSHEET_COURIER_NAME
                        FROM v_inbound {where_sql} AND {card_query_filter}
                        ORDER BY age_days DESC
                        """
                    )

                    if not df_card_detail_full.empty:
                        st.dataframe(
                            df_card_detail_full.head(200),
                            use_container_width=True,
                            height=290,
                        )
                        st.caption(
                            f"ℹ️ Menampilkan 200 baris teratas di layar (hemat RAM). Total data: **{len(df_card_detail_full):,} AWB**."
                        )
                        c_dl, c_rst = st.columns([3, 1])
                        with c_dl:
                            st.download_button(
                                label=f"📥 Download CSV Lengkap [{card_title}] ({len(df_card_detail_full):,} AWB)",
                                data=df_card_detail_full.to_csv(index=False).encode(
                                    "utf-8"
                                ),
                                file_name=f"Detail_{card_title}_Full.csv",
                                mime="text/csv",
                                use_container_width=True,
                            )
                        with c_rst:
                            if st.button(
                                "✖ Tutup Filter", use_container_width=True
                            ):
                                st.session_state.active_card_filter = None
                                gc.collect()
                                st.rerun()
                    else:
                        st.info("Tidak ada data untuk kategori ini.")

            st.markdown("---")
            view_choice = st.radio(
                "📌 Mode Visualisasi Operasional:",
                ["📊 Analisis Infografis UnRunsheet", "⚠️ Tabel UnInbound"],
                horizontal=True,
            )

            if view_choice == "📊 Analisis Infografis UnRunsheet":
                st.markdown(
                    '<div class="section-banner">📊 Visualisasi Komposisi & Distribusi Usia SLA (Aging)</div>',
                    unsafe_allow_html=True,
                )
                unrunsheet_status_clause = (
                    ""
                    if chosen_status
                    else "AND Status_Shipment IN ('UnRunsheet', 'UnRunsheet with PRA')"
                )

                df_aging = run_query(
                    f"""
                    SELECT aging_group, COUNT(DISTINCT AWB) AS total_awb
                    FROM v_inbound {where_sql} {unrunsheet_status_clause}
                    GROUP BY 1 ORDER BY CASE aging_group WHEN 'H+0' THEN 0 WHEN 'H+1' THEN 1 WHEN 'H+2' THEN 2 WHEN 'H+3' THEN 3 WHEN 'H+4' THEN 4 WHEN 'H+5' THEN 5 WHEN 'H+6' THEN 6 WHEN 'H+7' THEN 7 WHEN '>H+7' THEN 8 END
                    """
                )

                # Interactive Aging Selector Strip
                st.caption(
                    "🎯 **Filter Cepat Kategori Aging** (Klik kategori untuk memfilter tabel wilayah & detail di bawah):"
                )
                card_cols = st.columns(len(AGING_ORDER) + 1)
                with card_cols[0]:
                    if st.button("🔄 All", use_container_width=True):
                        st.session_state.selected_aging_card = None
                        st.rerun()

                for idx, ag in enumerate(AGING_ORDER):
                    with card_cols[idx + 1]:
                        match_row = (
                            df_aging[df_aging["aging_group"] == ag]["total_awb"].values
                            if not df_aging.empty
                            else [0]
                        )
                        vol_ag = int(match_row[0]) if len(match_row) > 0 else 0
                        is_selected = st.session_state.selected_aging_card == ag
                        btn_label = (
                            f"✨ {ag}\n({vol_ag:,})"
                            if is_selected
                            else f"{ag}\n({vol_ag:,})"
                        )
                        if st.button(
                            btn_label, key=f"card_ag_{ag}", use_container_width=True
                        ):
                            st.session_state.selected_aging_card = ag
                            st.rerun()

                selected_aging_filter = st.session_state.selected_aging_card
                if selected_aging_filter:
                    st.info(
                        f"📌 Filter Aging Aktif: **{selected_aging_filter}** (Klik 'All' untuk reset)"
                    )

                # 2-Column Infographic Charts: Left = Status Composition Donut, Right = SLA Aging Bar
                chart_c1, chart_c2 = st.columns([1, 1.55])

                with chart_c1:
                    comp_labels = [
                        "Success / Return",
                        "UnRunsheet",
                        "UnInbound",
                        "Undelivery",
                    ]
                    comp_values = [
                        total_success,
                        total_unrunsheet,
                        total_uninbound,
                        total_undel,
                    ]
                    if sum(comp_values) > 0:
                        fig_donut = px.pie(
                            names=comp_labels,
                            values=comp_values,
                            hole=0.58,
                            color=comp_labels,
                            color_discrete_map={
                                "Success / Return": "#28a745",
                                "UnRunsheet": "#dc3545",
                                "UnInbound": "#ffc107",
                                "Undelivery": "#6366f1",
                            },
                        )
                        fig_donut.update_traces(
                            textposition="inside",
                            textinfo="percent",
                            hovertemplate="<b>%{label}</b><br>Volume: %{value:,} AWB<br>Porsi: %{percent}<extra></extra>",
                        )
                        fig_donut.update_layout(
                            title=dict(
                                text="<b>Komposisi Status Inbound</b>",
                                x=0,
                                font=dict(size=15, color="#f8fafc"),
                            ),
                            annotations=[
                                dict(
                                    text=f"<b>{total_awb:,}</b><br><span style='font-size:11px;color:#94a3b8'>Total AWB</span>",
                                    x=0.5,
                                    y=0.5,
                                    font_size=16,
                                    font_color="#f8fafc",
                                    showarrow=False,
                                )
                            ],
                            legend=dict(
                                orientation="h",
                                yanchor="bottom",
                                y=-0.22,
                                xanchor="center",
                                x=0.5,
                                font=dict(size=11, color="#cbd5e1"),
                            ),
                            paper_bgcolor="rgba(15, 23, 42, 0.65)",
                            plot_bgcolor="rgba(0,0,0,0)",
                            margin=dict(l=12, r=12, t=45, b=45),
                            height=350,
                        )
                        st.plotly_chart(fig_donut, use_container_width=True)

                with chart_c2:
                    if not df_aging.empty:
                        fig_bar = px.bar(
                            df_aging,
                            x="aging_group",
                            y="total_awb",
                            text="total_awb",
                            color="aging_group",
                            color_discrete_map=AGING_COLOR_MAP,
                            labels={"aging_group": "", "total_awb": "Jumlah AWB"},
                        )
                        fig_bar.update_traces(
                            texttemplate="%{text:,}",
                            textposition="outside",
                            textfont=dict(color="#f8fafc", size=12),
                            cliponaxis=False,
                        )
                        fig_bar.update_layout(
                            title=dict(
                                text="<b>Distribusi Aging UnRunsheet (SLA Scale)</b>",
                                x=0,
                                font=dict(size=15, color="#f8fafc"),
                            ),
                            showlegend=False,
                            xaxis=dict(
                                categoryorder="array",
                                categoryarray=AGING_ORDER,
                                tickfont=dict(color="#cbd5e1"),
                            ),
                            yaxis=dict(
                                showgrid=True,
                                gridcolor="rgba(148, 163, 184, 0.15)",
                                tickfont=dict(color="#cbd5e1"),
                            ),
                            plot_bgcolor="rgba(15, 23, 42, 0.65)",
                            paper_bgcolor="rgba(15, 23, 42, 0.65)",
                            margin=dict(l=15, r=15, t=45, b=20),
                            height=350,
                        )
                        st.plotly_chart(fig_bar, use_container_width=True)

                # Top 5 Kecamatan Bottleneck Horizontal Infographic Bar
                if not top_kec_df.empty:
                    top_kec_sorted = top_kec_df.sort_values("total", ascending=True)
                    fig_top = px.bar(
                        top_kec_sorted,
                        x="total",
                        y="Kecamatan",
                        orientation="h",
                        text="total",
                        color="total",
                        color_continuous_scale=["#ffc107", "#dc3545"],
                    )
                    fig_top.update_traces(
                        texttemplate=" %{text:,} AWB",
                        textposition="outside",
                        textfont=dict(color="#f8fafc", size=12),
                        cliponaxis=False,
                    )
                    fig_top.update_layout(
                        title=dict(
                            text="<b>🔥 Top 5 Kecamatan Dengan Backlog UnRunsheet Tertinggi</b>",
                            x=0,
                            font=dict(size=14, color="#f8fafc"),
                        ),
                        coloraxis_showscale=False,
                        xaxis=dict(
                            showgrid=True,
                            gridcolor="rgba(148, 163, 184, 0.15)",
                            title="",
                            tickfont=dict(color="#cbd5e1"),
                        ),
                        yaxis=dict(title="", tickfont=dict(color="#f1f5f9", size=12)),
                        plot_bgcolor="rgba(15, 23, 42, 0.65)",
                        paper_bgcolor="rgba(15, 23, 42, 0.65)",
                        margin=dict(l=15, r=55, t=40, b=15),
                        height=230,
                    )
                    st.plotly_chart(fig_top, use_container_width=True)

                aging_click_clause = (
                    f"AND aging_group = '{selected_aging_filter}'"
                    if selected_aging_filter
                    else ""
                )
                st.markdown("---")

                with st.expander(
                    "📊 Tabel Matrix Status Shipment (Klik untuk Buka/Tutup)",
                    expanded=False,
                ):
                    pivot_matrix = run_query(
                        f"""
                        PIVOT (SELECT Status_Shipment, aging_group, AWB FROM v_inbound {where_sql})
                        ON aging_group IN ('H+0', 'H+1', 'H+2', 'H+3', 'H+4', 'H+5', 'H+6', 'H+7', '>H+7')
                        USING COUNT(DISTINCT AWB) GROUP BY Status_Shipment
                        """
                    )
                    if not pivot_matrix.empty:
                        pivot_matrix = pivot_matrix.fillna(0)
                        numeric_cols = [
                            c for c in pivot_matrix.columns if c != "Status_Shipment"
                        ]
                        pivot_matrix["Grand Total"] = pivot_matrix[numeric_cols].sum(
                            axis=1
                        )
                        st.dataframe(pivot_matrix, use_container_width=True)

                with st.expander(
                    "🗂️ UnRunsheet By Last Location, Cabang, Zona, Kecamatan",
                    expanded=True,
                ):
                    group_level = st.radio(
                        "📌 Pilih Level Tampilan:",
                        [
                            "Detail (Lokasi, Cabang, Zona, Kecamatan)",
                            "Ringkas per Cabang",
                            "Ringkas per Zona",
                        ],
                        horizontal=True,
                    )
                    group_cols = (
                        ["LAST_LOCATION", "Cabang", "Zona", "Kecamatan"]
                        if group_level == "Detail (Lokasi, Cabang, Zona, Kecamatan)"
                        else (
                            ["Cabang"]
                            if group_level == "Ringkas per Cabang"
                            else ["Cabang", "Zona"]
                        )
                    )
                    select_cols_sql = ", ".join(group_cols)

                    pivot_loc = run_query(
                        f"""
                        PIVOT (SELECT {select_cols_sql}, aging_group, AWB FROM v_inbound {where_sql} {unrunsheet_status_clause} {aging_click_clause})
                        ON aging_group IN ('H+0', 'H+1', 'H+2', 'H+3', 'H+4', 'H+5', 'H+6', 'H+7', '>H+7')
                        USING COUNT(DISTINCT AWB) GROUP BY {select_cols_sql}
                        """
                    )

                    if not pivot_loc.empty:
                        pivot_loc = pivot_loc.fillna(0)
                        for col in AGING_ORDER:
                            if col not in pivot_loc.columns:
                                pivot_loc[col] = 0
                        pivot_loc["Grand Total"] = pivot_loc[AGING_ORDER].sum(axis=1)
                        pivot_loc = pivot_loc.sort_values(
                            by="Grand Total", ascending=False
                        )

                        def highlight_red_alert(val):
                            return (
                                "color: #ff4d4d; font-weight: bold;"
                                if isinstance(val, (int, float)) and val > 0
                                else "color: #888888;"
                            )

                        red_alert_cols = [
                            c
                            for c in ["H+3", "H+4", "H+5", "H+6", "H+7", ">H+7"]
                            if c in pivot_loc.columns
                        ]
                        st.dataframe(
                            pivot_loc.style.map(
                                highlight_red_alert, subset=red_alert_cols
                            ),
                            use_container_width=True,
                        )

                # RAM Optimization: Lazy load raw AWB table only when requested
                with st.expander(
                    "📋 Detail AWB UnRunsheet & Export CSV (On-Demand Hemat RAM)",
                    expanded=False,
                ):
                    load_detail_unrun = st.checkbox(
                        "✅ Muat Data Detail AWB ke Memori (Klik untuk menampilkan tabel & CSV)",
                        value=False,
                        key="chk_load_detail_unrun",
                    )
                    if load_detail_unrun:
                        df_detail_full = run_query(
                            f"""
                            SELECT AWB, Last_Inbound, WUS_DATE, RUNSHEET_NO, payment_type, age_days AS "Aging (Hari)", LAST_LOCATION, Cabang, Kecamatan, SERVICE, Status_Shipment, HRS, POD_Category, POD_DESC, WUS_REMARKS, RUNSHEET_COURIER_NAME
                            FROM v_inbound {where_sql} {unrunsheet_status_clause} {aging_click_clause}
                            ORDER BY age_days DESC
                            """
                        )
                        if not df_detail_full.empty:
                            st.dataframe(
                                df_detail_full.head(200), use_container_width=True
                            )
                            st.caption(
                                f"ℹ️ Ditampilkan 200 baris teratas di layar. Total data: **{len(df_detail_full):,} AWB**."
                            )
                            st.download_button(
                                label=f"📥 Download CSV Lengkap Detail UnRunsheet ({len(df_detail_full):,} AWB)",
                                data=df_detail_full.to_csv(index=False).encode("utf-8"),
                                file_name="Detail_UnRunsheet_Full.csv",
                                mime="text/csv",
                            )
                            del df_detail_full
                            gc.collect()

            else:
                st.markdown(
                    '<div class="section-banner">⚠️ Detail Data UnInbound</div>',
                    unsafe_allow_html=True,
                )
                df_uninbound_full = run_query(
                    f"""
                    SELECT AWB, Cabang, Zona, Kecamatan, RUNSHEET_NO, payment_type, age_days AS "Aging from OM/HVO (Hari)", SERVICE, Status_Shipment, HRS
                    FROM v_inbound {where_sql} AND Status_Shipment = 'UnInbound'
                    ORDER BY age_days DESC
                    """
                )
                if not df_uninbound_full.empty:
                    st.dataframe(
                        df_uninbound_full.head(200), use_container_width=True
                    )
                    st.caption(
                        f"ℹ️ Ditampilkan 200 baris teratas di layar. Total data UnInbound: **{len(df_uninbound_full):,} AWB**."
                    )
                    st.download_button(
                        label=f"📥 Download CSV Lengkap UnInbound ({len(df_uninbound_full):,} AWB)",
                        data=df_uninbound_full.to_csv(index=False).encode("utf-8"),
                        file_name="UnInbound_Full.csv",
                        mime="text/csv",
                    )
                    del df_uninbound_full
                    gc.collect()
                else:
                    st.success("🎉 Tidak ada backlog UnInbound pada filter saat ini.")

    # =========================================================
    # TAB 2: SUMMARY EXEC
    # =========================================================
    with tab2:
        if summary_pass == EXEC_PASSWORD:
            with st.spinner("⏳ Memuat Executive Summary & Analytics..."):
                st.markdown(
                    '<div class="section-banner">📊 Executive SLA Scorecard & Performance Analytics</div>',
                    unsafe_allow_html=True,
                )

                avail_weeks_df = run_query(
                    f"SELECT DISTINCT pod_week_num as wk_num FROM v_inbound {where_sql} AND pod_week_num IS NOT NULL ORDER BY wk_num DESC"
                )
                week_list = (
                    avail_weeks_df["wk_num"].dropna().astype(int).tolist()
                    if not avail_weeks_df.empty
                    else [43]
                )
                week_options = ["All Week"] + week_list

                col_w_sel, _ = st.columns([1, 2])
                with col_w_sel:
                    selected_week_opt = st.selectbox(
                        "📅 Pilih Minggu Analysis (Week):",
                        options=week_options,
                        index=0,
                        key="exec_selected_week",
                    )

                week_clause = (
                    "AND pod_week_num IS NOT NULL"
                    if selected_week_opt == "All Week"
                    else f"AND pod_week_num = {int(selected_week_opt)}"
                )

                kpi_batch_df = run_query(
                    f"""
                    SELECT 
                        COUNT(DISTINCT CASE WHEN Status_Shipment IN ('SUCCESS DELIVERY', 'SUCCESS RETURN SHIPPER') AND (HRS = 'UnHRS' OR HRS = '-' OR HRS IS NULL) AND UPPER(COALESCE(RUNSHEET_NO, '')) LIKE '%MXG%' AND UPPER(TRIM(Zona)) IN ('A', 'B') THEN AWB END) AS succ_ab_vol,
                        COUNT(DISTINCT CASE WHEN Status_Shipment IN ('SUCCESS DELIVERY', 'SUCCESS RETURN SHIPPER') AND UPPER(COALESCE(RUNSHEET_NO, '')) LIKE '%MXG%' AND UPPER(TRIM(Zona)) IN ('A', 'B') THEN AWB END) AS succ_ab_curr_tot,
                        COUNT(DISTINCT CASE WHEN Status_Shipment IN ('SUCCESS DELIVERY', 'SUCCESS RETURN SHIPPER') AND UPPER(COALESCE(RUNSHEET_NO, '')) LIKE '%MXG%' AND UPPER(TRIM(Zona)) IN ('A', 'B') AND HRS IN ('HRS', 'HOC', 'HRI') AND hrs_aging_group = 'H+0' THEN AWB END) AS succ_ab_curr_tgt,
                        
                        COUNT(DISTINCT CASE WHEN Status_Shipment IN ('SUCCESS DELIVERY', 'SUCCESS RETURN SHIPPER') AND (HRS = 'UnHRS' OR HRS = '-' OR HRS IS NULL) AND UPPER(COALESCE(RUNSHEET_NO, '')) LIKE '%MXG%' AND UPPER(TRIM(Zona)) = 'C' THEN AWB END) AS succ_c_vol,
                        COUNT(DISTINCT CASE WHEN Status_Shipment IN ('SUCCESS DELIVERY', 'SUCCESS RETURN SHIPPER') AND UPPER(COALESCE(RUNSHEET_NO, '')) LIKE '%MXG%' AND UPPER(TRIM(Zona)) = 'C' THEN AWB END) AS succ_c_curr_tot,
                        COUNT(DISTINCT CASE WHEN Status_Shipment IN ('SUCCESS DELIVERY', 'SUCCESS RETURN SHIPPER') AND UPPER(COALESCE(RUNSHEET_NO, '')) LIKE '%MXG%' AND UPPER(TRIM(Zona)) = 'C' AND HRS IN ('HRS', 'HOC', 'HRI') AND hrs_aging_group IN ('H+0', 'H+1') THEN AWB END) AS succ_c_curr_tgt,

                        COUNT(DISTINCT CASE WHEN Status_Shipment = 'UNDELIVERY' AND (HRS = 'UnHRS' OR HRS = '-' OR HRS IS NULL) AND UPPER(COALESCE(RUNSHEET_NO, '')) LIKE '%MXG%' AND UPPER(TRIM(Zona)) IN ('A', 'B') THEN AWB END) AS undel_ab_vol,
                        COUNT(DISTINCT CASE WHEN Status_Shipment = 'UNDELIVERY' AND UPPER(TRIM(Zona)) IN ('A', 'B') THEN AWB END) AS undel_ab_curr_tot,
                        COUNT(DISTINCT CASE WHEN Status_Shipment = 'UNDELIVERY' AND UPPER(TRIM(Zona)) IN ('A', 'B') AND HRS IN ('HRS', 'HOC', 'HRI') AND hrs_aging_group = 'H+0' THEN AWB END) AS undel_ab_curr_tgt,

                        COUNT(DISTINCT CASE WHEN Status_Shipment = 'UNDELIVERY' AND (HRS = 'UnHRS' OR HRS = '-' OR HRS IS NULL) AND UPPER(COALESCE(RUNSHEET_NO, '')) LIKE '%MXG%' AND UPPER(TRIM(Zona)) = 'C' THEN AWB END) AS undel_c_vol,
                        COUNT(DISTINCT CASE WHEN Status_Shipment = 'UNDELIVERY' AND UPPER(TRIM(Zona)) = 'C' THEN AWB END) AS undel_c_curr_tot,
                        COUNT(DISTINCT CASE WHEN Status_Shipment = 'UNDELIVERY' AND UPPER(TRIM(Zona)) = 'C' AND HRS IN ('HRS', 'HOC', 'HRI') AND hrs_aging_group IN ('H+0', 'H+1') THEN AWB END) AS undel_c_curr_tgt
                    FROM v_inbound {where_sql} {week_clause}
                    """
                )

                if not kpi_batch_df.empty:
                    r = kpi_batch_df.iloc[0]
                    succ_ab_pct = (
                        (r["succ_ab_curr_tgt"] / r["succ_ab_curr_tot"] * 100.0)
                        if r["succ_ab_curr_tot"] > 0
                        else 0.0
                    )
                    succ_c_pct = (
                        (r["succ_c_curr_tgt"] / r["succ_c_curr_tot"] * 100.0)
                        if r["succ_c_curr_tot"] > 0
                        else 0.0
                    )
                    undel_ab_pct = (
                        (r["undel_ab_curr_tgt"] / r["undel_ab_curr_tot"] * 100.0)
                        if r["undel_ab_curr_tot"] > 0
                        else 0.0
                    )
                    undel_c_pct = (
                        (r["undel_c_curr_tgt"] / r["undel_c_curr_tot"] * 100.0)
                        if r["undel_c_curr_tot"] > 0
                        else 0.0
                    )

                    def render_exec_scorecard(title, vol, pct):
                        is_pass = pct >= 95.0
                        accent = "#28a745" if is_pass else "#dc3545"
                        status_txt = "ON TARGET (≥95%)" if is_pass else "BELOW TARGET"
                        return render_kpi_card(
                            title=title,
                            value_str=f"{int(vol):,} AWB",
                            sub_str=f"Capaian SLA HRS: <b>{pct:.1f}%</b> (Target: 95.0%)",
                            accent_color=accent,
                            pill_text=status_txt,
                            progress_pct=pct,
                        )

                    kc1, kc2, kc3, kc4 = st.columns(4)
                    kc1.markdown(
                        render_exec_scorecard(
                            "Success UnHRS (Zona A&B | H+0)",
                            r["succ_ab_vol"] or 0,
                            succ_ab_pct,
                        ),
                        unsafe_allow_html=True,
                    )
                    kc2.markdown(
                        render_exec_scorecard(
                            "Success UnHRS (Zona C | ≤ H+1)",
                            r["succ_c_vol"] or 0,
                            succ_c_pct,
                        ),
                        unsafe_allow_html=True,
                    )
                    kc3.markdown(
                        render_exec_scorecard(
                            "Undel UnHRS (Zona A&B | H+0)",
                            r["undel_ab_vol"] or 0,
                            undel_ab_pct,
                        ),
                        unsafe_allow_html=True,
                    )
                    kc4.markdown(
                        render_exec_scorecard(
                            "Undel UnHRS (Zona C | ≤ H+1)",
                            r["undel_c_vol"] or 0,
                            undel_c_pct,
                        ),
                        unsafe_allow_html=True,
                    )

                st.markdown("---")

                # =========================================================
                # GRAFIK TREN VOLUME INBOUND + 1-CLICK PERIODE SELECTOR
                # =========================================================
                c_tr_1, c_tr_2 = st.columns([1.8, 1.4])
                with c_tr_1:
                    st.subheader("📈 Tren Volume Inbound (Tahun 2026)")
                with c_tr_2:
                    period_options = [
                        "Harian (Daily)",
                        "Mingguan (Weekly)",
                        "Bulanan (Monthly)",
                    ]
                    if hasattr(st, "segmented_control"):
                        trend_view_mode = st.segmented_control(
                            "Pilih Periode Grafik:",
                            options=period_options,
                            default="Harian (Daily)",
                            label_visibility="collapsed",
                            key="trend_period_selector",
                        )
                        if not trend_view_mode:
                            trend_view_mode = "Harian (Daily)"
                    else:
                        trend_view_mode = st.radio(
                            "Pilih Periode Grafik:",
                            options=period_options,
                            index=0,
                            horizontal=True,
                            label_visibility="collapsed",
                            key="trend_period_selector_radio",
                        )

                enable_day_filter = st.checkbox(
                    "📌 Aktifkan Filter Rentang Tanggal (Day Range 1–31)", value=False
                )
                if enable_day_filter:
                    col_d1, col_d2 = st.columns(2)
                    with col_d1:
                        start_day_val = st.number_input(
                            "Dari Tanggal:", min_value=1, max_value=31, value=1
                        )
                    with col_d2:
                        end_day_val = st.number_input(
                            "Sampai Tanggal:", min_value=1, max_value=31, value=31
                        )
                    day_filter_clause = (
                        f"AND EXTRACT(DAY FROM TRY_STRPTIME(Last_Inbound, '%m/%d/%Y %H:%M')) BETWEEN {start_day_val} AND {end_day_val}"
                        if start_day_val <= end_day_val
                        else ""
                    )
                else:
                    day_filter_clause = ""

                sql_date_expr = "TRY_STRPTIME(Last_Inbound, '%m/%d/%Y %H:%M')"

                if trend_view_mode == "Harian (Daily)":
                    sql_period_expr = f"CAST({sql_date_expr} AS DATE)"
                    sql_trend_group = f"STRFTIME({sql_date_expr}, '%d %b %Y') AS periode, {sql_period_expr} as sort_key"
                    group_by_clause = "GROUP BY 1, 2 ORDER BY sort_key ASC"
                elif trend_view_mode == "Mingguan (Weekly)":
                    sql_period_expr = f"EXTRACT(WEEK FROM {sql_date_expr})"
                    sql_trend_group = f"CONCAT('Week ', CAST(CAST({sql_period_expr} AS INTEGER) AS STRING)) AS periode, {sql_period_expr} as sort_key"
                    group_by_clause = (
                        f"AND {sql_date_expr} IS NOT NULL GROUP BY 1, 2 ORDER BY sort_key ASC"
                    )
                else:
                    sql_period_expr = f"STRFTIME({sql_date_expr}, '%Y-%m')"
                    sql_trend_group = f"STRFTIME({sql_date_expr}, '%b %Y') AS periode, {sql_period_expr} as sort_key"
                    group_by_clause = "GROUP BY 1, 2 ORDER BY sort_key ASC"

                df_inbound_trend = run_query(
                    f"""
                    SELECT {sql_trend_group}, COUNT(DISTINCT AWB) AS total_awb
                    FROM v_inbound
                    {where_sql_trend} AND EXTRACT(YEAR FROM TRY_STRPTIME(Last_Inbound, '%m/%d/%Y %H:%M')) = 2026 {day_filter_clause}
                    {group_by_clause}
                    """
                )

                if not df_inbound_trend.empty:
                    avg_vol = df_inbound_trend["total_awb"].mean()
                    fig_inbound = px.area(
                        df_inbound_trend,
                        x="periode",
                        y="total_awb",
                        markers=True,
                        labels={"periode": "", "total_awb": "Total AWB"},
                    )
                    fig_inbound.update_traces(
                        line=dict(color="#38bdf8", width=3),
                        fillcolor="rgba(56, 189, 248, 0.18)",
                        marker=dict(
                            size=6,
                            color="#f8fafc",
                            line=dict(width=2, color="#38bdf8"),
                        ),
                    )
                    fig_inbound.add_hline(
                        y=avg_vol,
                        line_dash="dash",
                        line_color="#ffc107",
                        annotation_text=f"Rata-rata: {avg_vol:,.0f} AWB",
                        annotation_font_color="#ffc107",
                    )
                    fig_inbound.update_layout(
                        plot_bgcolor="rgba(15, 23, 42, 0.7)",
                        paper_bgcolor="rgba(15, 23, 42, 0.7)",
                        font=dict(color="#cbd5e1"),
                        xaxis=dict(showgrid=False),
                        yaxis=dict(
                            showgrid=True, gridcolor="rgba(148, 163, 184, 0.15)"
                        ),
                        margin=dict(l=15, r=15, t=25, b=15),
                        height=340,
                    )
                    st.plotly_chart(fig_inbound, use_container_width=True)
                else:
                    st.info("Tidak ada data tren inbound pada periode yang dipilih.")

                st.markdown("---")
                st.subheader(f"📋 Tabel Produktivitas Cabang ({trend_view_mode})")

                df_pivot_raw = run_query(
                    f"""
                    SELECT Cabang, {sql_period_expr} AS period_raw, COUNT(DISTINCT AWB) AS total_awb
                    FROM v_inbound 
                    {where_sql_trend} AND Cabang IN ('Malang', 'Batu', 'Blitar', 'Kepanjen', 'Bululawang', 'Turen')
                      AND {sql_period_expr} IS NOT NULL AND EXTRACT(YEAR FROM TRY_STRPTIME(Last_Inbound, '%m/%d/%Y %H:%M')) = 2026 {day_filter_clause}
                    GROUP BY Cabang, period_raw ORDER BY period_raw ASC
                    """
                )

                if not df_pivot_raw.empty:
                    if trend_view_mode == "Harian (Daily)":
                        df_pivot_raw["periode"] = pd.to_datetime(
                            df_pivot_raw["period_raw"]
                        ).dt.strftime("%d %b %Y")
                    elif trend_view_mode == "Mingguan (Weekly)":
                        df_pivot_raw["periode"] = "Week " + df_pivot_raw[
                            "period_raw"
                        ].astype(int).astype(str)
                    else:
                        df_pivot_raw["periode"] = (
                            pd.to_datetime(
                                df_pivot_raw["period_raw"].astype(str) + "-01",
                                errors="coerce",
                            )
                            .dt.strftime("%b %Y")
                            .fillna(df_pivot_raw["period_raw"])
                        )

                    main_cabangs = [
                        "Malang",
                        "Blitar",
                        "Batu",
                        "Kepanjen",
                        "Turen",
                        "Bululawang",
                    ]
                    periods_ordered = df_pivot_raw["periode"].unique().tolist()

                    pivot_data = []
                    for cab in main_cabangs:
                        row_dict = {"Cabang": cab}
                        cab_df = df_pivot_raw[df_pivot_raw["Cabang"] == cab]
                        prev_val = None
                        for p in periods_ordered:
                            match_val = cab_df[cab_df["periode"] == p][
                                "total_awb"
                            ].values
                            curr_val = int(match_val[0]) if len(match_val) > 0 else 0
                            row_dict[f"{p} (AWB)"] = curr_val
                            row_dict[f"{p} (% Growth)"] = (
                                f"{((curr_val - prev_val) / prev_val) * 100.0:+.2f}%"
                                if prev_val and prev_val > 0
                                else "0.00%"
                            )
                            prev_val = curr_val if curr_val > 0 else prev_val
                        pivot_data.append(row_dict)

                    df_final_pivot = pd.DataFrame(pivot_data)
                    st.dataframe(df_final_pivot, use_container_width=True, height=265)
                    st.download_button(
                        "📥 Download CSV Produktivitas Cabang (Pivot)",
                        df_final_pivot.to_csv(index=False).encode("utf-8"),
                        "Produktivitas_Cabang_Pivot.csv",
                        "text/csv",
                    )

                st.markdown("---")

                # =========================================================
                # MATRIX INFOGRAFIS WARNA BERDASARKAN USIA SLA (H+0..>H+7)
                # H+0 = Hijau | H+1 & H+2 = Kuning | H+3 s/d >H+7 = Merah
                # =========================================================
                def render_sla_matrix_table(
                    df_raw, title_text, csv_filename, download_label
                ):
                    if df_raw.empty:
                        st.info("Tidak ada data pada filter terpilih.")
                        return

                    pivot = df_raw.pivot(
                        index="Cabang", columns="aging_group", values="total_awb"
                    ).fillna(0)
                    for col in AGING_ORDER:
                        if col not in pivot.columns:
                            pivot[col] = 0

                    pivot = pivot[AGING_ORDER]
                    pivot["Grand Total"] = pivot[AGING_ORDER].sum(axis=1)
                    pivot = pivot.sort_values(by="Grand Total", ascending=False)

                    grand_total_row = pivot.sum(axis=0)
                    grand_total_row.name = "Grand Total"

                    pivot_with_total = (
                        pd.concat([pivot, pd.DataFrame([grand_total_row])])
                        .reset_index()
                        .rename(columns={"index": "Cabang"})
                    )
                    headers = list(pivot_with_total.columns)

                    cell_values = [
                        [
                            f"<b>{v}</b>"
                            for v in pivot_with_total[col].astype(str).tolist()
                        ]
                        if col == "Cabang"
                        else [
                            f"<b>{int(val):,}</b>" if val > 0 else "0"
                            for val in pivot_with_total[col].tolist()
                        ]
                        for col in headers
                    ]

                    cell_colors, font_colors = [], []
                    n_rows = len(pivot_with_total)

                    for col in headers:
                        col_color_list, col_font_list = [], []
                        for row_idx in range(n_rows):
                            val = pivot_with_total[col].iloc[row_idx]
                            # Baris paling bawah (Grand Total)
                            if row_idx == n_rows - 1:
                                col_color_list.append("#0f172a")
                                col_font_list.append(
                                    "#38bdf8" if col == "Grand Total" else "#ffffff"
                                )
                            # Kolom Cabang & Kolom Grand Total
                            elif col in ["Cabang", "Grand Total"]:
                                col_color_list.append("#1e293b")
                                col_font_list.append(
                                    "#38bdf8" if col == "Grand Total" else "#ffffff"
                                )
                            # Kolom Aging SLA (Warna berdasarkan usia SLA jika volume > 0)
                            else:
                                if val <= 0:
                                    col_color_list.append("#1e293b")
                                    col_font_list.append("#64748b")
                                elif col == "H+0":
                                    col_color_list.append("#28a745")  # Hijau
                                    col_font_list.append("#ffffff")
                                elif col in ["H+1", "H+2"]:
                                    col_color_list.append("#ffc107")  # Kuning
                                    col_font_list.append("#000000")
                                elif col in [
                                    "H+3",
                                    "H+4",
                                    "H+5",
                                    "H+6",
                                    "H+7",
                                    ">H+7",
                                ]:
                                    col_color_list.append("#dc3545")  # Merah
                                    col_font_list.append("#ffffff")
                                else:
                                    col_color_list.append("#1e293b")
                                    col_font_list.append("#ffffff")
                        cell_colors.append(col_color_list)
                        font_colors.append(col_font_list)

                    st.markdown(
                        """
                        <div class="sla-legend">
                            <span><i class="dot-green"></i><b>H+0</b>: Hijau (Fresh / On-Time)</span>
                            <span><i class="dot-yellow"></i><b>H+1 – H+2</b>: Kuning (Warning SLA)</span>
                            <span><i class="dot-red"></i><b>H+3 – &gt;H+7</b>: Merah (Overdue / Critical)</span>
                            <span><i class="dot-zero"></i><b>0</b>: Tidak Ada Backlog</span>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )

                    fig_table = go.Figure(
                        data=[
                            go.Table(
                                columnwidth=[1.3] + [0.85] * (len(headers) - 2) + [1.1],
                                header=dict(
                                    values=[f"<b>{h}</b>" for h in headers],
                                    fill_color="#0f172a",
                                    line_color="#334155",
                                    align=["left" if h == "Cabang" else "center" for h in headers],
                                    font=dict(color="#f8fafc", size=13),
                                    height=34,
                                ),
                                cells=dict(
                                    values=cell_values,
                                    fill_color=cell_colors,
                                    line_color="#334155",
                                    align=["left" if h == "Cabang" else "center" for h in headers],
                                    font=dict(color=font_colors, size=12),
                                    height=30,
                                ),
                            )
                        ]
                    )
                    fig_table.update_layout(
                        title=dict(
                            text=f"<b>{title_text}</b>",
                            x=0,
                            font=dict(size=15, color="#f8fafc"),
                        ),
                        margin=dict(l=5, r=5, t=42, b=5),
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)",
                        height=max(260, (n_rows + 1) * 32 + 55),
                    )

                    st.plotly_chart(fig_table, use_container_width=True)
                    st.download_button(
                        download_label,
                        pivot_with_total.to_csv(index=False).encode("utf-8"),
                        csv_filename,
                        "text/csv",
                    )

                with st.expander(
                    "📊 2. Matrix Infografis UnRunsheet Per Cabang", expanded=True
                ):
                    df_unrun = run_query(
                        f"SELECT Cabang, aging_group, COUNT(DISTINCT AWB) AS total_awb FROM v_inbound {where_sql} AND Status_Shipment IN ('UnRunsheet', 'UnRunsheet with PRA') GROUP BY Cabang, aging_group"
                    )
                    render_sla_matrix_table(
                        df_unrun,
                        "Matrix SLA UnRunsheet Per Cabang",
                        "Matrix_UnRunsheet_Cabang.csv",
                        "📥 Download Data Matrix UnRunsheet (CSV)",
                    )

                with st.expander(
                    "📊 3. Matrix Infografis Success UnHRS Per Cabang", expanded=False
                ):
                    df_succ = run_query(
                        f"SELECT Cabang, wus_aging_group AS aging_group, COUNT(DISTINCT AWB) AS total_awb FROM v_inbound {where_sql} {week_clause} AND Status_Shipment IN ('SUCCESS DELIVERY', 'SUCCESS RETURN SHIPPER') AND (HRS = 'UnHRS' OR HRS = '-' OR HRS IS NULL) AND UPPER(COALESCE(RUNSHEET_NO, '')) LIKE '%MXG%' GROUP BY Cabang, wus_aging_group"
                    )
                    render_sla_matrix_table(
                        df_succ,
                        "Matrix SLA Success UnHRS Per Cabang",
                        "Matrix_Success_UnHRS_Cabang.csv",
                        "📥 Download Data Matrix Success UnHRS (CSV)",
                    )

                with st.expander(
                    "📊 4. Matrix Infografis Undel UnHRS Per Cabang", expanded=False
                ):
                    df_undel = run_query(
                        f"SELECT Cabang, wus_aging_group AS aging_group, COUNT(DISTINCT AWB) AS total_awb FROM v_inbound {where_sql} {week_clause} AND Status_Shipment = 'UNDELIVERY' AND (HRS = 'UnHRS' OR HRS = '-' OR HRS IS NULL) AND UPPER(COALESCE(RUNSHEET_NO, '')) LIKE '%MXG%' GROUP BY Cabang, wus_aging_group"
                    )
                    render_sla_matrix_table(
                        df_undel,
                        "Matrix SLA Undel UnHRS Per Cabang",
                        "Matrix_Undel_UnHRS_Cabang.csv",
                        "📥 Download Data Matrix Undel UnHRS (CSV)",
                    )

        else:
            st.warning(
                "🔐 Masukkan password 'Summary Exec' yang benar di sidebar kiri untuk melihat halaman ini."
            )

except Exception as main_err:
    st.error("⚠️ Terjadi masalah saat mengolah data dashboard:")
    st.exception(main_err)
