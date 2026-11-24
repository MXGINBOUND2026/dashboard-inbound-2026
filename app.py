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
    """<style>
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
    font-size: 11.5px;
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
    font-size: 26px;
    font-weight: 800;
    color: #f8fafc;
    line-height: 1.15;
    margin: 4px 0 6px 0;
}
.kpi-sub {
    font-size: 11.5px;
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
.kpi-ref-box {
    background: rgba(15, 23, 42, 0.85);
    border: 1px solid #334155;
    border-left: 4px solid #38bdf8;
    padding: 8px 14px;
    border-radius: 8px;
    font-size: 12px;
    color: #e2e8f0;
    margin-bottom: 8px;
}
.dot-green { display:inline-block; width:10px; height:10px; background:#28a745; border-radius:2px; margin-right:5px; }
.dot-yellow { display:inline-block; width:10px; height:10px; background:#ffc107; border-radius:2px; margin-right:5px; }
.dot-red { display:inline-block; width:10px; height:10px; background:#dc3545; border-radius:2px; margin-right:5px; }
.dot-blue { display:inline-block; width:10px; height:10px; background:#0284c7; border-radius:2px; margin-right:5px; }
.dot-zero { display:inline-block; width:10px; height:10px; background:#1e293b; border:1px solid #475569; border-radius:2px; margin-right:5px; }
</style>""",
    unsafe_allow_html=True,
)

TARGET_GDRIVE_FOLDER_ID = "1qLpozZT-UX2Wm_6F6FzCIjESUkEgbMaP"
OUTPUT_PARQUET_NAME = "data_inbound_2026.parquet"
LOCAL_PARQUET_PATH = os.path.join(os.getcwd(), OUTPUT_PARQUET_NAME)

AGING_ORDER = ["H+0", "H+1", "H+2", "H+3", "H+4", "H+5", "H+6", "H+7", ">H+7"]
AGING_COLOR_MAP = {
    "H+0": "#28a745",
    "H+1": "#ffc107",
    "H+2": "#ffc107",
    "H+3": "#dc3545",
    "H+4": "#dc3545",
    "H+5": "#dc3545",
    "H+6": "#dc3545",
    "H+7": "#dc3545",
    ">H+7": "#dc3545",
}

LT_RUNNING_BUCKETS = [
    "H+0 jam 00:00-12:00",
    "H+0 jam 12:01-17:00",
    "H+0 jam 17:01-23:59",
    "H+1",
    "H+2",
    "H+3",
    "H+4",
    "H+5",
    "H+6",
    "H+7",
    ">H+7",
]

H0_BUCKETS = [
    "H+0 jam 00:00-12:00",
    "H+0 jam 12:01-17:00",
    "H+0 jam 17:01-23:59",
]

# =========================================================
# KONFIGURASI TARGET KPI TERPUSAT (SESUAI TABEL STANDAR KPI)
# Hanya Zona A, B, dan C (Tanpa Zona D)
# =========================================================
KPI_TARGETS_CONFIG = {
    "REG": {
        "Non COD": {
            "A": {"H+0": 95.0, "H+1": 98.0},
            "B": {"H+1": 95.0, "H+2": 98.0},
            "C": {"H+1": 95.0, "H+2": 98.0},
        },
        "COD": {
            "A": {"H+1": 90.0, "H+3": 95.0},
            "B": {"H+2": 90.0, "H+4": 95.0},
            "C": {"H+2": 90.0, "H+4": 95.0},
        },
    },
    "JTR": {
        "Non COD": {
            "A": {"H+0": 95.0, "H+1": 98.0},
            "B": {"H+1": 95.0, "H+2": 98.0},
            "C": {"H+2": 95.0, "H+3": 98.0},
        },
        "COD": {
            "A": {"H+1": 90.0, "H+3": 95.0},
            "B": {"H+2": 90.0, "H+4": 95.0},
            "C": {"H+2": 90.0, "H+4": 95.0},
        },
    },
    "CTC": {"H+0": 95.0, "H+1": 99.0},
    "YES": {"H+0": 95.0, "H+1": 99.0},
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


# Helper HTML Infographic Card (Inline zero-indentation HTML)
def render_kpi_card(
    title,
    value_str,
    sub_str,
    accent_color="#38bdf8",
    pill_text=None,
    progress_pct=None,
    value_color="#f8fafc",
):
    pct_clamped = (
        max(0.0, min(100.0, float(progress_pct)))
        if progress_pct is not None
        else 0.0
    )
    pill_html = (
        f'<span class="kpi-pill" style="background:{accent_color}22; color:{accent_color}; border:1px solid {accent_color}66;">{pill_text}</span>'
        if pill_text
        else ""
    )
    bar_html = (
        f'<div class="progress-track"><div class="progress-fill" style="width:{pct_clamped:.1f}%; background:{accent_color};"></div></div>'
        if progress_pct is not None
        else ""
    )
    return (
        f'<div class="kpi-card" style="border-top: 3px solid {accent_color};">'
        f'<div class="kpi-top"><span class="kpi-label">{title}</span>{pill_html}</div>'
        f'<div class="kpi-value" style="color:{value_color};">{value_str}</div>'
        f'<div class="kpi-sub">{sub_str}</div>'
        f"{bar_html}"
        f"</div>"
    )


def get_kpi_status_visual(actual_pct, target_pct):
    diff = actual_pct - target_pct
    if actual_pct >= target_pct:
        return ("#28a745", f"ON TARGET ({diff:+.2f}%)", "#4ade80")
    elif actual_pct >= (target_pct - 5.0):
        return ("#ffc107", f"WARNING ({diff:+.2f}%)", "#facc15")
    else:
        return ("#dc3545", f"BELOW ({diff:+.2f}%)", "#f87171")


# =========================================================
# HELPER KPI TARGET & ALERT COLOR ENGINE (RUNNING-IN-TOTAL)
# =========================================================
def get_kpi_targets_for_row(mode, zona_code, pay_mode="Non COD", service_group="REG"):
    z = str(zona_code).strip().upper()
    if z not in ("A", "B", "C"):
        z = "B"

    if mode in ("CTC", "YES"):
        raw_t = KPI_TARGETS_CONFIG[mode]
    else:
        svc_key = "JTR" if service_group == "JTR" else "REG"
        pay_key = "COD" if pay_mode == "COD" else "Non COD"
        raw_t = KPI_TARGETS_CONFIG[svc_key][pay_key][z]

    expanded_targets = {}
    for k, v in raw_t.items():
        if k == "H+0":
            for hb in H0_BUCKETS:
                expanded_targets[hb] = v
        else:
            expanded_targets[k] = v
    return expanded_targets


def evaluate_kpi_cell_style(val_pct, bucket_name, targets_dict, is_parent_row=False):
    if pd.isna(val_pct):
        return ("#0f172a" if is_parent_row else "#1e293b", "#64748b")

    final_target = max(targets_dict.values()) if targets_dict else 95.0
    target_indices = [
        LT_RUNNING_BUCKETS.index(k)
        for k in targets_dict.keys()
        if k in LT_RUNNING_BUCKETS
    ]
    max_target_idx = max(target_indices) if target_indices else 3
    curr_idx = (
        LT_RUNNING_BUCKETS.index(bucket_name)
        if bucket_name in LT_RUNNING_BUCKETS
        else max_target_idx + 1
    )

    if bucket_name in targets_dict:
        tgt = targets_dict[bucket_name]
        if val_pct >= tgt:
            return ("#15803d" if is_parent_row else "#28a745", "#ffffff")
        elif val_pct >= (tgt - 5.0):
            return (
                "#b45309" if is_parent_row else "#ffc107",
                "#ffffff" if is_parent_row else "#000000",
            )
        else:
            return ("#991b1b" if is_parent_row else "#dc3545", "#ffffff")
    elif curr_idx < max_target_idx:
        next_targets = [
            targets_dict[LT_RUNNING_BUCKETS[i]]
            for i in range(curr_idx + 1, max_target_idx + 1)
            if LT_RUNNING_BUCKETS[i] in targets_dict
        ]
        next_tgt = next_targets[0] if next_targets else final_target
        if val_pct >= next_tgt:
            return ("#15803d" if is_parent_row else "#28a745", "#ffffff")
        elif val_pct >= (next_tgt - 10.0):
            return (
                "#b45309" if is_parent_row else "#ffc107",
                "#ffffff" if is_parent_row else "#000000",
            )
        else:
            return ("#7f1d1d" if is_parent_row else "#991b1b", "#fecaca")
    else:
        if val_pct >= final_target:
            return ("#0369a1" if is_parent_row else "#0284c7", "#ffffff")
        elif val_pct >= (final_target - 5.0):
            return (
                "#b45309" if is_parent_row else "#ffc107",
                "#ffffff" if is_parent_row else "#000000",
            )
        else:
            return ("#991b1b" if is_parent_row else "#dc3545", "#ffffff")


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
    schema_cols = (
        ensure_view().execute("DESCRIBE v_inbound").fetchdf()["column_name"].tolist()
    )
    has_new_schema = all(
        col in schema_cols
        for col in ["Type_Shipment", "lt_inbound_bucket", "lt_entry_bucket"]
    )

    st.sidebar.markdown("---")
    st.sidebar.header("🔍 Filter Data Dashboard")

    @st.cache_data(ttl=3600, show_spinner=False)
    def get_filter_options(schema_ready):
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
                "SELECT DISTINCT Zona FROM v_inbound WHERE Zona IS NOT NULL AND UPPER(TRIM(Zona)) != 'D' ORDER BY 1"
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
        types = (
            con.execute(
                "SELECT DISTINCT Type_Shipment FROM v_inbound WHERE Type_Shipment IS NOT NULL ORDER BY 1"
            )
            .fetchdf()["Type_Shipment"]
            .tolist()
            if schema_ready
            else []
        )

        minmax = con.execute(
            """
            SELECT MIN(TRY_STRPTIME(Last_Inbound, '%m/%d/%Y %H:%M')), MAX(TRY_STRPTIME(Last_Inbound, '%m/%d/%Y %H:%M')) 
            FROM v_inbound WHERE Last_Inbound IS NOT NULL AND Last_Inbound != '-'
            """
        ).fetchdf()

        return locs, cab, zon, kec, stat, hrs, types, minmax

    (
        unique_locations,
        unique_cabang_db,
        unique_zona,
        unique_kecamatan,
        unique_status,
        unique_hrs,
        unique_type_shipment,
        min_max_date,
    ) = get_filter_options(has_new_schema)

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
        chosen_type = (
            st.multiselect("🏷️ Filter Type Shipment:", unique_type_shipment)
            if has_new_schema
            else []
        )

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
    if chosen_type and has_new_schema:
        where_clauses_general.append(build_in_clause("Type_Shipment", chosen_type))

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
        f'<div class="hero-header">'
        f'<div><div class="hero-title">🚚 Dashboard Inbound 2026</div>'
        f'<div class="hero-subtitle">Monitoring SLA Runsheet, UnInbound, & Kepatuhan HRS Real-Time</div></div>'
        f'<div class="update-badge">🕒 Last Update: {formatted_date}</div>'
        f"</div>",
        unsafe_allow_html=True,
    )

    if not has_new_schema:
        st.warning(
            "⚠️ File Parquet saat ini belum memiliki kolom `Type_Shipment` & `TGL_ENTRY`. "
            "Jalankan ulang `convert_to_parquet.py` lalu klik **🔄 Refresh & Tarik Data Terbaru** di sidebar."
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
                f'<div class="marquee-container-tab1">'
                f'<div class="marquee-text-tab1">🔥 TOP 5 BOTTLENECK UNRUNSHEET: &nbsp;&nbsp;{top_kecamatan_str}</div>'
                f"</div>",
                unsafe_allow_html=True,
            )

            if "active_view" not in st.session_state:
                st.session_state.active_view = "UnRunsheet"
            if "active_card_filter" not in st.session_state:
                st.session_state.active_card_filter = None
            if "selected_aging_card" not in st.session_state:
                st.session_state.selected_aging_card = None

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

                    type_col_sql = "Type_Shipment, " if has_new_schema else ""
                    df_card_detail_full = run_query(
                        f"""
                        SELECT AWB, {type_col_sql}Last_Inbound, TGL_UPDATE_STATUS_POD, RUNSHEET_NO, payment_type, age_days AS "Aging (Hari)", LAST_LOCATION, Cabang, Zona, Kecamatan, SERVICE, Status_Shipment, HRS, RUNSHEET_COURIER_NAME
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
                    '<div class="section-banner">📊 Visualisasi Komposisi Zona & Distribusi Usia SLA (Aging)</div>',
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

                chart_c1, chart_c2 = st.columns([1, 1.55])

                with chart_c1:
                    df_unrun_zona = run_query(
                        f"""
                        SELECT 
                            UPPER(TRIM(Zona)) AS zona_code,
                            COUNT(DISTINCT AWB) AS total_awb
                        FROM v_inbound {where_sql}
                        AND Status_Shipment IN ('UnRunsheet', 'UnRunsheet with PRA')
                        AND UPPER(TRIM(Zona)) IN ('A', 'B', 'C')
                        GROUP BY 1
                        """
                    )

                    zona_map = (
                        dict(zip(df_unrun_zona["zona_code"], df_unrun_zona["total_awb"]))
                        if not df_unrun_zona.empty
                        else {}
                    )
                    zona_labels = ["Zona A", "Zona B", "Zona C"]
                    zona_values = [
                        int(zona_map.get("A", 0)),
                        int(zona_map.get("B", 0)),
                        int(zona_map.get("C", 0)),
                    ]
                    total_unrun_zona = sum(zona_values)

                    if total_unrun_zona > 0:
                        fig_donut = px.pie(
                            names=zona_labels,
                            values=zona_values,
                            hole=0.58,
                            color=zona_labels,
                            color_discrete_map={
                                "Zona A": "#38bdf8",
                                "Zona B": "#ffc107",
                                "Zona C": "#dc3545",
                            },
                        )
                        fig_donut.update_traces(
                            sort=False,
                            textposition="inside",
                            textinfo="percent",
                            hovertemplate="<b>%{label}</b><br>UnRunsheet (+PRA): %{value:,} AWB<br>Porsi: %{percent}<extra></extra>",
                        )
                        fig_donut.update_layout(
                            title=dict(
                                text="<b>Komposisi UnRunsheet per Zona</b>",
                                x=0,
                                font=dict(size=15, color="#f8fafc"),
                            ),
                            annotations=[
                                dict(
                                    text=f"<b>{total_unrun_zona:,}</b><br><span style='font-size:11px;color:#94a3b8'>Total UnRunsheet</span>",
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
                                font=dict(size=12, color="#cbd5e1"),
                            ),
                            paper_bgcolor="rgba(15, 23, 42, 0.65)",
                            plot_bgcolor="rgba(0,0,0,0)",
                            margin=dict(l=12, r=12, t=45, b=45),
                            height=350,
                        )
                        st.plotly_chart(fig_donut, use_container_width=True)
                    else:
                        st.info("Tidak ada data UnRunsheet pada Zona A, B, atau C.")

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
                    STATUS_CAT_OPTIONS = ["Monitoring", "Close", "OTS Firstmile"]
                    if "matrix_status_mode" not in st.session_state:
                        st.session_state.matrix_status_mode = None

                    if st.session_state.matrix_status_mode:
                        col_m_title, col_m_opt, col_m_rst = st.columns([1.7, 1.9, 0.6])
                    else:
                        col_m_title, col_m_opt = st.columns([1.7, 2.5])
                        col_m_rst = None

                    with col_m_title:
                        st.markdown("#### 📊 Matrix Status Shipment")

                    with col_m_opt:
                        if hasattr(st, "segmented_control"):
                            selected_mode = st.segmented_control(
                                "Kategori Status:",
                                options=STATUS_CAT_OPTIONS,
                                default=st.session_state.matrix_status_mode,
                                label_visibility="collapsed",
                                key="matrix_status_seg",
                            )
                        else:
                            selected_mode = st.radio(
                                "Kategori Status:",
                                options=STATUS_CAT_OPTIONS,
                                index=STATUS_CAT_OPTIONS.index(
                                    st.session_state.matrix_status_mode
                                )
                                if st.session_state.matrix_status_mode
                                in STATUS_CAT_OPTIONS
                                else None,
                                horizontal=True,
                                label_visibility="collapsed",
                                key="matrix_status_radio",
                            )

                        if selected_mode != st.session_state.matrix_status_mode:
                            st.session_state.matrix_status_mode = selected_mode
                            st.rerun()

                    if col_m_rst is not None:
                        with col_m_rst:
                            if st.button(
                                "🔄 Reset",
                                key="btn_reset_matrix_status",
                                use_container_width=True,
                            ):
                                st.session_state.matrix_status_mode = None
                                if "matrix_status_seg" in st.session_state:
                                    del st.session_state["matrix_status_seg"]
                                if "matrix_status_radio" in st.session_state:
                                    del st.session_state["matrix_status_radio"]
                                st.rerun()

                    matrix_cat_clause = (
                        f"AND Status_Category = '{st.session_state.matrix_status_mode}'"
                        if st.session_state.matrix_status_mode
                        else ""
                    )

                    pivot_matrix = run_query(
                        f"""
                        PIVOT (SELECT Status_Shipment, aging_group, AWB FROM v_inbound {where_sql} {matrix_cat_clause})
                        ON aging_group IN ('H+0', 'H+1', 'H+2', 'H+3', 'H+4', 'H+5', 'H+6', 'H+7', '>H+7')
                        USING COUNT(DISTINCT AWB) GROUP BY Status_Shipment
                        """
                    )
                    if not pivot_matrix.empty:
                        pivot_matrix = pivot_matrix.fillna(0)
                        for col in AGING_ORDER:
                            if col not in pivot_matrix.columns:
                                pivot_matrix[col] = 0
                        pivot_matrix = pivot_matrix[["Status_Shipment"] + AGING_ORDER]
                        pivot_matrix["Grand Total"] = pivot_matrix[AGING_ORDER].sum(
                            axis=1
                        )
                        pivot_matrix = pivot_matrix.sort_values(
                            by="Grand Total", ascending=False
                        )
                        st.dataframe(pivot_matrix, use_container_width=True)
                    else:
                        st.info("Tidak ada data untuk kategori status tersebut.")

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
                        type_col_sql = "Type_Shipment, " if has_new_schema else ""
                        df_detail_full = run_query(
                            f"""
                            SELECT AWB, {type_col_sql}Last_Inbound, WUS_DATE, RUNSHEET_NO, payment_type, age_days AS "Aging (Hari)", LAST_LOCATION, Cabang, Kecamatan, SERVICE, Status_Shipment, HRS, POD_Category, POD_DESC, WUS_REMARKS, RUNSHEET_COURIER_NAME
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
                type_col_sql = "Type_Shipment, " if has_new_schema else ""
                df_uninbound_full = run_query(
                    f"""
                    SELECT AWB, {type_col_sql}Cabang, Zona, Kecamatan, RUNSHEET_NO, payment_type, age_days AS "Aging from OM/HVO (Hari)", SERVICE, Status_Shipment, HRS
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
                # GRAFIK SLA LEAD TIME (% RUNNING IN TOTAL) + KPI HIGHLIGHTS
                # PEMBAGI: TOTAL AWB TRANSACTION YANG ADA LAST_INBOUND
                # =========================================================
                st.markdown(
                    '<div class="section-banner">🎯 Lead Time SLA Delivery (% Running in Total vs Target KPI — Pembagi: Total Inbound TRANSACTION)</div>',
                    unsafe_allow_html=True,
                )

                if not has_new_schema:
                    st.info(
                        "Silakan perbarui file Parquet dengan `convert_to_parquet.py` terbaru untuk menampilkan 3 matriks Lead Time SLA (% Running in Total)."
                    )
                else:
                    # ---------------------------------------------------------
                    # KPI HIGHLIGHT CARDS PER SERVICE (REG vs JTR vs CTC vs YES)
                    # TANPA ZONA D
                    # ---------------------------------------------------------
                    col_hl_title, col_hl_zona = st.columns([2.1, 1.3])
                    with col_hl_title:
                        st.markdown(
                            "#### 🏆 Highlight Capaian KPI Lead Time (% Success dari Total Inbound TRANSACTION)"
                        )
                    with col_hl_zona:
                        hl_zona_opt = st.radio(
                            "Pilih Acuan Zona KPI Card:",
                            ["All Zona", "Zona A", "Zona B", "Zona C"],
                            index=0,
                            horizontal=True,
                            key="hl_zona_selector",
                        )

                    hl_zona_sql = (
                        "AND UPPER(TRIM(COALESCE(Zona, ''))) IN ('A', 'B', 'C')"
                        if hl_zona_opt == "All Zona"
                        else f"AND UPPER(TRIM(COALESCE(Zona, ''))) = '{hl_zona_opt.replace('Zona ', '')}'"
                    )
                    hl_z_ref = (
                        "A"
                        if hl_zona_opt in ("All Zona", "Zona A")
                        else hl_zona_opt.replace("Zona ", "")
                    )

                    h0_in_sql = "'H+0 jam 00:00-12:00', 'H+0 jam 12:01-17:00', 'H+0 jam 17:01-23:59'"
                    h1_in_sql = f"{h0_in_sql}, 'H+1'"
                    h2_in_sql = f"{h1_in_sql}, 'H+2'"
                    h3_in_sql = f"{h2_in_sql}, 'H+3'"
                    h4_in_sql = f"{h3_in_sql}, 'H+4'"

                    reg_cond = "UPPER(COALESCE(SERVICE, '')) LIKE '%REG%'"
                    jtr_cond = "(UPPER(COALESCE(SERVICE, '')) LIKE '%JTR%' OR UPPER(COALESCE(SERVICE, '')) LIKE '%TRC%')"
                    ctc_cond = "UPPER(COALESCE(SERVICE, '')) LIKE '%CTC%'"
                    yes_cond = "UPPER(COALESCE(SERVICE, '')) LIKE '%YES%'"
                    succ_cond = "Status_Shipment = 'SUCCESS DELIVERY'"

                    hl_query = f"""
                        SELECT
                            -- REG Non COD
                            COUNT(DISTINCT CASE WHEN {reg_cond} AND payment_type = 'Non COD' THEN AWB END) AS reg_ncod_tot,
                            COUNT(DISTINCT CASE WHEN {succ_cond} AND {reg_cond} AND payment_type = 'Non COD' AND lt_inbound_bucket IN ({h0_in_sql}) THEN AWB END) AS reg_ncod_h0,
                            COUNT(DISTINCT CASE WHEN {succ_cond} AND {reg_cond} AND payment_type = 'Non COD' AND lt_inbound_bucket IN ({h1_in_sql}) THEN AWB END) AS reg_ncod_h1,
                            COUNT(DISTINCT CASE WHEN {succ_cond} AND {reg_cond} AND payment_type = 'Non COD' AND lt_inbound_bucket IN ({h2_in_sql}) THEN AWB END) AS reg_ncod_h2,

                            -- REG COD
                            COUNT(DISTINCT CASE WHEN {reg_cond} AND payment_type = 'COD' THEN AWB END) AS reg_cod_tot,
                            COUNT(DISTINCT CASE WHEN {succ_cond} AND {reg_cond} AND payment_type = 'COD' AND lt_inbound_bucket IN ({h1_in_sql}) THEN AWB END) AS reg_cod_h1,
                            COUNT(DISTINCT CASE WHEN {succ_cond} AND {reg_cond} AND payment_type = 'COD' AND lt_inbound_bucket IN ({h2_in_sql}) THEN AWB END) AS reg_cod_h2,
                            COUNT(DISTINCT CASE WHEN {succ_cond} AND {reg_cond} AND payment_type = 'COD' AND lt_inbound_bucket IN ({h3_in_sql}) THEN AWB END) AS reg_cod_h3,
                            COUNT(DISTINCT CASE WHEN {succ_cond} AND {reg_cond} AND payment_type = 'COD' AND lt_inbound_bucket IN ({h4_in_sql}) THEN AWB END) AS reg_cod_h4,

                            -- JTR (JTR & TRC) Non COD
                            COUNT(DISTINCT CASE WHEN {jtr_cond} AND payment_type = 'Non COD' THEN AWB END) AS jtr_ncod_tot,
                            COUNT(DISTINCT CASE WHEN {succ_cond} AND {jtr_cond} AND payment_type = 'Non COD' AND lt_inbound_bucket IN ({h0_in_sql}) THEN AWB END) AS jtr_ncod_h0,
                            COUNT(DISTINCT CASE WHEN {succ_cond} AND {jtr_cond} AND payment_type = 'Non COD' AND lt_inbound_bucket IN ({h1_in_sql}) THEN AWB END) AS jtr_ncod_h1,
                            COUNT(DISTINCT CASE WHEN {succ_cond} AND {jtr_cond} AND payment_type = 'Non COD' AND lt_inbound_bucket IN ({h2_in_sql}) THEN AWB END) AS jtr_ncod_h2,
                            COUNT(DISTINCT CASE WHEN {succ_cond} AND {jtr_cond} AND payment_type = 'Non COD' AND lt_inbound_bucket IN ({h3_in_sql}) THEN AWB END) AS jtr_ncod_h3,

                            -- JTR (JTR & TRC) COD
                            COUNT(DISTINCT CASE WHEN {jtr_cond} AND payment_type = 'COD' THEN AWB END) AS jtr_cod_tot,
                            COUNT(DISTINCT CASE WHEN {succ_cond} AND {jtr_cond} AND payment_type = 'COD' AND lt_inbound_bucket IN ({h1_in_sql}) THEN AWB END) AS jtr_cod_h1,
                            COUNT(DISTINCT CASE WHEN {succ_cond} AND {jtr_cond} AND payment_type = 'COD' AND lt_inbound_bucket IN ({h2_in_sql}) THEN AWB END) AS jtr_cod_h2,
                            COUNT(DISTINCT CASE WHEN {succ_cond} AND {jtr_cond} AND payment_type = 'COD' AND lt_inbound_bucket IN ({h3_in_sql}) THEN AWB END) AS jtr_cod_h3,
                            COUNT(DISTINCT CASE WHEN {succ_cond} AND {jtr_cond} AND payment_type = 'COD' AND lt_inbound_bucket IN ({h4_in_sql}) THEN AWB END) AS jtr_cod_h4,

                            -- CTC (Entry -> Delivered | Pembagi: Total Inbound TRANSACTION)
                            COUNT(DISTINCT CASE WHEN {ctc_cond} THEN AWB END) AS ctc_tot,
                            COUNT(DISTINCT CASE WHEN {succ_cond} AND {ctc_cond} AND lt_entry_bucket IN ({h0_in_sql}) THEN AWB END) AS ctc_h0,
                            COUNT(DISTINCT CASE WHEN {succ_cond} AND {ctc_cond} AND lt_entry_bucket IN ({h1_in_sql}) THEN AWB END) AS ctc_h1,

                            -- YES (Entry -> Delivered | Pembagi: Total Inbound TRANSACTION)
                            COUNT(DISTINCT CASE WHEN {yes_cond} THEN AWB END) AS yes_tot,
                            COUNT(DISTINCT CASE WHEN {succ_cond} AND {yes_cond} AND lt_entry_bucket IN ({h0_in_sql}) THEN AWB END) AS yes_h0,
                            COUNT(DISTINCT CASE WHEN {succ_cond} AND {yes_cond} AND lt_entry_bucket IN ({h1_in_sql}) THEN AWB END) AS yes_h1
                        FROM v_inbound
                        {where_sql}
                          AND Type_Shipment = 'TRANSACTION'
                          AND Last_Inbound IS NOT NULL AND Last_Inbound != '-'
                          AND TRY_STRPTIME(Last_Inbound, '%m/%d/%Y %H:%M') IS NOT NULL
                          {hl_zona_sql}
                    """

                    df_hl = run_query(hl_query)
                    if not df_hl.empty:
                        hr = df_hl.iloc[0]

                        def calc_pct(num_col, den_col):
                            den = float(hr[den_col] or 0)
                            num = float(hr[num_col] or 0)
                            return (num / den * 100.0) if den > 0 else 0.0, int(den)

                        if hl_z_ref == "A":
                            reg_ncod_m1 = ("H+0", calc_pct("reg_ncod_h0", "reg_ncod_tot"), 95.0)
                            reg_ncod_m2 = ("H+1", calc_pct("reg_ncod_h1", "reg_ncod_tot"), 98.0)
                            reg_cod_m1 = ("H+1", calc_pct("reg_cod_h1", "reg_cod_tot"), 90.0)
                            reg_cod_m2 = ("H+3", calc_pct("reg_cod_h3", "reg_cod_tot"), 95.0)

                            jtr_ncod_m1 = ("H+0", calc_pct("jtr_ncod_h0", "jtr_ncod_tot"), 95.0)
                            jtr_ncod_m2 = ("H+1", calc_pct("jtr_ncod_h1", "jtr_ncod_tot"), 98.0)
                            jtr_cod_m1 = ("H+1", calc_pct("jtr_cod_h1", "jtr_cod_tot"), 90.0)
                            jtr_cod_m2 = ("H+3", calc_pct("jtr_cod_h3", "jtr_cod_tot"), 95.0)
                        elif hl_z_ref == "B":
                            reg_ncod_m1 = ("H+1", calc_pct("reg_ncod_h1", "reg_ncod_tot"), 95.0)
                            reg_ncod_m2 = ("H+2", calc_pct("reg_ncod_h2", "reg_ncod_tot"), 98.0)
                            reg_cod_m1 = ("H+2", calc_pct("reg_cod_h2", "reg_cod_tot"), 90.0)
                            reg_cod_m2 = ("H+4", calc_pct("reg_cod_h4", "reg_cod_tot"), 95.0)

                            jtr_ncod_m1 = ("H+1", calc_pct("jtr_ncod_h1", "jtr_ncod_tot"), 95.0)
                            jtr_ncod_m2 = ("H+2", calc_pct("jtr_ncod_h2", "jtr_ncod_tot"), 98.0)
                            jtr_cod_m1 = ("H+2", calc_pct("jtr_cod_h2", "jtr_cod_tot"), 90.0)
                            jtr_cod_m2 = ("H+4", calc_pct("jtr_cod_h4", "jtr_cod_tot"), 95.0)
                        else:  # Zona C
                            reg_ncod_m1 = ("H+1", calc_pct("reg_ncod_h1", "reg_ncod_tot"), 95.0)
                            reg_ncod_m2 = ("H+2", calc_pct("reg_ncod_h2", "reg_ncod_tot"), 98.0)
                            reg_cod_m1 = ("H+2", calc_pct("reg_cod_h2", "reg_cod_tot"), 90.0)
                            reg_cod_m2 = ("H+4", calc_pct("reg_cod_h4", "reg_cod_tot"), 95.0)

                            jtr_ncod_m1 = ("H+2", calc_pct("jtr_ncod_h2", "jtr_ncod_tot"), 95.0)
                            jtr_ncod_m2 = ("H+3", calc_pct("jtr_ncod_h3", "jtr_ncod_tot"), 98.0)
                            jtr_cod_m1 = ("H+2", calc_pct("jtr_cod_h2", "jtr_cod_tot"), 90.0)
                            jtr_cod_m2 = ("H+4", calc_pct("jtr_cod_h4", "jtr_cod_tot"), 95.0)

                        def build_hl_card_html(card_title, milestone_lbl, pct_vol_tuple, target_val):
                            actual_val, vol_cnt = pct_vol_tuple
                            accent_c, pill_txt, val_c = get_kpi_status_visual(
                                actual_val, target_val
                            )
                            return render_kpi_card(
                                title=f"{card_title} ({milestone_lbl})",
                                value_str=f"{actual_val:.2f}%",
                                sub_str=f"Target: <b>{target_val:.0f}%</b> &nbsp;|&nbsp; Inbound TRX: <b>{vol_cnt:,}</b> AWB",
                                accent_color=accent_c,
                                pill_text=pill_txt,
                                progress_pct=actual_val,
                                value_color=val_c,
                            )

                        # Baris 1: KPI Highlight Service REG
                        r1_c1, r1_c2, r1_c3, r1_c4 = st.columns(4)
                        r1_c1.markdown(
                            build_hl_card_html(
                                "REG Non COD", reg_ncod_m1[0], reg_ncod_m1[1], reg_ncod_m1[2]
                            ),
                            unsafe_allow_html=True,
                        )
                        r1_c2.markdown(
                            build_hl_card_html(
                                "REG Non COD", reg_ncod_m2[0], reg_ncod_m2[1], reg_ncod_m2[2]
                            ),
                            unsafe_allow_html=True,
                        )
                        r1_c3.markdown(
                            build_hl_card_html(
                                "REG COD", reg_cod_m1[0], reg_cod_m1[1], reg_cod_m1[2]
                            ),
                            unsafe_allow_html=True,
                        )
                        r1_c4.markdown(
                            build_hl_card_html(
                                "REG COD", reg_cod_m2[0], reg_cod_m2[1], reg_cod_m2[2]
                            ),
                            unsafe_allow_html=True,
                        )

                        # Baris 2: KPI Highlight Service JTR (JTR & TRC)
                        r2_c1, r2_c2, r2_c3, r2_c4 = st.columns(4)
                        r2_c1.markdown(
                            build_hl_card_html(
                                "JTR Non COD", jtr_ncod_m1[0], jtr_ncod_m1[1], jtr_ncod_m1[2]
                            ),
                            unsafe_allow_html=True,
                        )
                        r2_c2.markdown(
                            build_hl_card_html(
                                "JTR Non COD", jtr_ncod_m2[0], jtr_ncod_m2[1], jtr_ncod_m2[2]
                            ),
                            unsafe_allow_html=True,
                        )
                        r2_c3.markdown(
                            build_hl_card_html(
                                "JTR COD", jtr_cod_m1[0], jtr_cod_m1[1], jtr_cod_m1[2]
                            ),
                            unsafe_allow_html=True,
                        )
                        r2_c4.markdown(
                            build_hl_card_html(
                                "JTR COD", jtr_cod_m2[0], jtr_cod_m2[1], jtr_cod_m2[2]
                            ),
                            unsafe_allow_html=True,
                        )

                        # Baris 3: KPI Highlight CTC (Intracity) & YES
                        r3_c1, r3_c2, r3_c3, r3_c4 = st.columns(4)
                        r3_c1.markdown(
                            build_hl_card_html(
                                "CTC Intracity", "H+0", calc_pct("ctc_h0", "ctc_tot"), 95.0
                            ),
                            unsafe_allow_html=True,
                        )
                        r3_c2.markdown(
                            build_hl_card_html(
                                "CTC Intracity", "H+1", calc_pct("ctc_h1", "ctc_tot"), 99.0
                            ),
                            unsafe_allow_html=True,
                        )
                        r3_c3.markdown(
                            build_hl_card_html(
                                "YES Express", "H+0", calc_pct("yes_h0", "yes_tot"), 95.0
                            ),
                            unsafe_allow_html=True,
                        )
                        r3_c4.markdown(
                            build_hl_card_html(
                                "YES Express", "H+1", calc_pct("yes_h1", "yes_tot"), 99.0
                            ),
                            unsafe_allow_html=True,
                        )

                    st.markdown("<div style='height:8px;'></div>", unsafe_allow_html=True)

                    # ---------------------------------------------------------
                    # FUNGSI MATRIX & S-CURVE % RUNNING IN TOTAL
                    # PEMBAGI: TOTAL AWB TRANSACTION YANG ADA LAST_INBOUND
                    # PEMBILANG: KUMULATIF AWB SUCCESS DELIVERY PER H+
                    # ---------------------------------------------------------
                    def render_running_total_sla_block(
                        block_key,
                        section_title,
                        mode,
                        base_ts_col,
                        bucket_col,
                        service_filter_sql,
                        default_expanded=True,
                    ):
                        with st.expander(section_title, expanded=default_expanded):
                            if mode == "INBOUND":
                                col_p1, col_p2, col_p3 = st.columns([1.4, 1.1, 1.3])
                            else:
                                col_p1, col_p2 = st.columns([1.5, 2.3])
                                col_p3 = None

                            with col_p1:
                                period_granularity = st.radio(
                                    "📅 Parameter Periode:",
                                    ["Tanggal (Daily)", "Week (Weekly)", "Month (Monthly)"],
                                    index=1,
                                    horizontal=True,
                                    key=f"gran_{block_key}",
                                )

                            if mode == "INBOUND":
                                with col_p2:
                                    pay_type_opt = st.radio(
                                        "💳 Type Kiriman:",
                                        ["Non COD", "COD", "All"],
                                        index=0,
                                        horizontal=True,
                                        key=f"pay_{block_key}",
                                    )
                                with col_p3:
                                    svc_rute_opt = st.radio(
                                        "🚚 Service (KPI Berbeda):",
                                        ["REG", "JTR (JTR & TRC)", "All"],
                                        index=0,
                                        horizontal=True,
                                        key=f"svc_{block_key}",
                                    )
                            else:
                                pay_type_opt = "All"
                                svc_rute_opt = "All"

                            svc_group_key = (
                                "JTR" if svc_rute_opt == "JTR (JTR & TRC)" else "REG"
                            )

                            # Kotak Referensi Target KPI & Rumus Pembagi (Tanpa Zona D)
                            denom_note = (
                                "<br><span style='color:#38bdf8;'>📐 <b>Rumus:</b> (Akumulasi AWB <code>SUCCESS DELIVERY</code> s/d H+ terkait) &divide; "
                                "(Total AWB <code>Type_Shipment = TRANSACTION</code> yang sudah memiliki <code>Last_Inbound</code>)</span>"
                            )
                            if mode == "INBOUND":
                                if svc_group_key == "JTR":
                                    kpi_info_html = (
                                        "🎯 <b>Target KPI Service JTR (JTR &amp; TRC) — Latest Inbound - Delivered:</b><br>"
                                        "&bull; <b>Non COD:</b> Zona A (<b>H+0: 95%</b>, <b>H+1: 98%</b>) &nbsp;|&nbsp; "
                                        "Zona B (<b>H+1: 95%</b>, <b>H+2: 98%</b>) &nbsp;|&nbsp; "
                                        "Zona C (<b>H+2: 95%</b>, <b>H+3: 98%</b>)<br>"
                                        "&bull; <b>COD:</b> Zona A (<b>H+1: 90%</b>, <b>H+3: 95%</b>) &nbsp;|&nbsp; "
                                        "Zona B &amp; C (<b>H+2: 90%</b>, <b>H+4: 95%</b>)"
                                        + denom_note
                                    )
                                else:
                                    kpi_info_html = (
                                        "🎯 <b>Target KPI Service Reguler (REG) — Latest Inbound - Delivered:</b><br>"
                                        "&bull; <b>Non COD:</b> Zona A (<b>H+0: 95%</b>, <b>H+1: 98%</b>) &nbsp;|&nbsp; "
                                        "Zona B &amp; C (<b>H+1: 95%</b>, <b>H+2: 98%</b>)<br>"
                                        "&bull; <b>COD:</b> Zona A (<b>H+1: 90%</b>, <b>H+3: 95%</b>) &nbsp;|&nbsp; "
                                        "Zona B &amp; C (<b>H+2: 90%</b>, <b>H+4: 95%</b>)"
                                        + denom_note
                                    )
                            elif mode == "CTC":
                                kpi_info_html = (
                                    "🎯 <b>Target KPI Entry/Handover - Delivered [CTC = Intracity]:</b> "
                                    "All Type Kiriman &amp; All Zone &rarr; <b>H+0: 95%</b> &nbsp;|&nbsp; <b>H+1: 99%</b>"
                                    + denom_note
                                )
                            else:
                                kpi_info_html = (
                                    "🎯 <b>Target KPI Entry/Handover - Delivered [Service YES]:</b> "
                                    "All Type Kiriman &amp; All Zone &rarr; <b>H+0: 95%</b> &nbsp;|&nbsp; <b>H+1: 99%</b>"
                                    + denom_note
                                )

                            if mode == "INBOUND":
                                st.markdown(
                                    f'<div class="kpi-ref-box">{kpi_info_html}</div>',
                                    unsafe_allow_html=True,
                                )
                            else:
                                with col_p2:
                                    st.markdown(
                                        f'<div class="kpi-ref-box">{kpi_info_html}</div>',
                                        unsafe_allow_html=True,
                                    )

                            extra_clauses = [
                                "Type_Shipment = 'TRANSACTION'",
                                "Last_Inbound IS NOT NULL AND Last_Inbound != '-'",
                                "TRY_STRPTIME(Last_Inbound, '%m/%d/%Y %H:%M') IS NOT NULL",
                            ]
                            if service_filter_sql:
                                extra_clauses.append(service_filter_sql)
                            if mode == "INBOUND":
                                if pay_type_opt in ("Non COD", "COD"):
                                    extra_clauses.append(
                                        f"payment_type = '{pay_type_opt}'"
                                    )
                                if svc_rute_opt == "REG":
                                    extra_clauses.append(
                                        "UPPER(COALESCE(SERVICE, '')) LIKE '%REG%'"
                                    )
                                elif svc_rute_opt == "JTR (JTR & TRC)":
                                    extra_clauses.append(
                                        "(UPPER(COALESCE(SERVICE, '')) LIKE '%JTR%' OR UPPER(COALESCE(SERVICE, '')) LIKE '%TRC%')"
                                    )

                            extra_sql = " AND ".join(extra_clauses)
                            ts_expr = f"TRY_STRPTIME({base_ts_col}, '%m/%d/%Y %H:%M')"

                            if period_granularity == "Tanggal (Daily)":
                                p_label_sql = f"STRFTIME({ts_expr}, '%d %b %Y')"
                                p_sort_sql = f"CAST({ts_expr} AS DATE)"
                                period_col_header = "TANGGAL"
                            elif period_granularity == "Week (Weekly)":
                                p_label_sql = f"CONCAT('Week ', CAST(CAST(EXTRACT(WEEK FROM {ts_expr}) AS INTEGER) AS STRING))"
                                p_sort_sql = f"EXTRACT(WEEK FROM {ts_expr})"
                                period_col_header = "WEEK of YEAR"
                            else:
                                p_label_sql = f"STRFTIME({ts_expr}, '%b %Y')"
                                p_sort_sql = f"STRFTIME({ts_expr}, '%Y-%m')"
                                period_col_header = "MONTH"

                            df_lt = run_query(
                                f"""
                                SELECT 
                                    {p_label_sql} AS periode,
                                    {p_sort_sql} AS sort_key,
                                    UPPER(TRIM(COALESCE(Zona, 'UNKNOWN'))) AS zona_code,
                                    CASE 
                                        WHEN Status_Shipment = 'SUCCESS DELIVERY' AND {bucket_col} IS NOT NULL THEN {bucket_col}
                                        ELSE 'NOT_DELIVERED'
                                    END AS lt_bucket,
                                    COUNT(DISTINCT AWB) AS awb_cnt
                                FROM v_inbound
                                {where_sql}
                                  AND {extra_sql}
                                  AND {ts_expr} IS NOT NULL
                                  AND UPPER(TRIM(COALESCE(Zona, ''))) IN ('A', 'B', 'C')
                                GROUP BY 1, 2, 3, 4
                                ORDER BY sort_key ASC, zona_code ASC
                                """
                            )

                            if df_lt.empty:
                                st.info(
                                    "Tidak ada data kiriman `Type_Shipment = TRANSACTION` yang sudah `Last_Inbound` pada filter yang dipilih."
                                )
                                return

                            def compute_running_row(df_slice, label_txt, zona_ref, is_parent):
                                total_vol = int(df_slice["awb_cnt"].sum())
                                bucket_counts = (
                                    df_slice.groupby("lt_bucket")["awb_cnt"]
                                    .sum()
                                    .to_dict()
                                )
                                cum_sum = 0
                                row_vals = {
                                    "Label": label_txt,
                                    "ZonaRef": zona_ref,
                                    "IsParent": is_parent,
                                    "VolAWB": total_vol,
                                }
                                for b in LT_RUNNING_BUCKETS:
                                    cnt = int(bucket_counts.get(b, 0))
                                    cum_sum += cnt
                                    pct = (
                                        (cum_sum / total_vol * 100.0)
                                        if total_vol > 0
                                        else 0.0
                                    )
                                    row_vals[b] = pct
                                row_vals["Total"] = (
                                    (cum_sum / total_vol * 100.0)
                                    if total_vol > 0
                                    else 0.0
                                )
                                return row_vals

                            periods_df = (
                                df_lt[["periode", "sort_key"]]
                                .drop_duplicates()
                                .sort_values("sort_key")
                            )

                            matrix_rows = []
                            curve_rows = []

                            for _, p_row in periods_df.iterrows():
                                p_name = p_row["periode"]
                                df_p = df_lt[df_lt["periode"] == p_name]

                                parent_r = compute_running_row(
                                    df_p, f"▪ {p_name}", "B", is_parent=True
                                )
                                matrix_rows.append(parent_r)

                                for z_code in ["A", "B", "C"]:
                                    df_pz = df_p[df_p["zona_code"] == z_code]
                                    if not df_pz.empty:
                                        child_r = compute_running_row(
                                            df_pz,
                                            f"    └ Zona {z_code}",
                                            z_code,
                                            is_parent=False,
                                        )
                                        matrix_rows.append(child_r)

                            gt_row = compute_running_row(
                                df_lt, "TOTAL KESELURUHAN", "B", is_parent=True
                            )
                            matrix_rows.append(gt_row)

                            for z_code in ["A", "B", "C"]:
                                df_z_all = df_lt[df_lt["zona_code"] == z_code]
                                if not df_z_all.empty:
                                    z_summary = compute_running_row(
                                        df_z_all, f"Zona {z_code}", z_code, False
                                    )
                                    for b in LT_RUNNING_BUCKETS:
                                        curve_rows.append(
                                            {
                                                "Zona": f"Zona {z_code}",
                                                "Bucket": b,
                                                "RunningPct": round(z_summary[b], 2),
                                            }
                                        )

                            df_matrix_disp = pd.DataFrame(matrix_rows)

                            st.markdown(
                                '<div class="sla-legend">'
                                '<span><i class="dot-green"></i><b>Hijau</b>: Tercapai (≥ Target KPI)</span>'
                                '<span><i class="dot-yellow"></i><b>Kuning</b>: Warning (Mendekati Target, -5%)</span>'
                                '<span><i class="dot-red"></i><b>Merah</b>: Di Bawah Target KPI</span>'
                                '<span><i class="dot-blue"></i><b>Biru</b>: Pasca Target Utama (Target Tercapai)</span>'
                                "</div>",
                                unsafe_allow_html=True,
                            )

                            vol_col_header = "Vol Inbound"
                            table_headers = (
                                [period_col_header, vol_col_header]
                                + LT_RUNNING_BUCKETS
                                + ["Total"]
                            )
                            cell_texts = []
                            cell_fills = []
                            cell_fonts = []

                            for col_name in table_headers:
                                col_txt_list = []
                                col_fill_list = []
                                col_font_list = []

                                for r_idx, r_data in df_matrix_disp.iterrows():
                                    is_parent = bool(r_data["IsParent"])
                                    is_grand_total = (
                                        r_idx == len(df_matrix_disp) - 1
                                    )
                                    z_ref = r_data["ZonaRef"]

                                    if col_name == period_col_header:
                                        col_txt_list.append(
                                            f"<b>{r_data['Label']}</b>"
                                            if is_parent
                                            else r_data["Label"]
                                        )
                                        col_fill_list.append(
                                            "#0f172a"
                                            if is_grand_total
                                            else ("#1e3a5f" if is_parent else "#1e293b")
                                        )
                                        col_font_list.append(
                                            "#38bdf8" if is_grand_total else "#f8fafc"
                                        )
                                    elif col_name == vol_col_header:
                                        vol_str = f"{int(r_data['VolAWB']):,}"
                                        col_txt_list.append(
                                            f"<b>{vol_str}</b>"
                                            if is_parent
                                            else vol_str
                                        )
                                        col_fill_list.append(
                                            "#0f172a"
                                            if is_grand_total
                                            else ("#1e3a5f" if is_parent else "#1e293b")
                                        )
                                        col_font_list.append("#cbd5e1")
                                    else:
                                        val_num = float(r_data[col_name])
                                        pct_str = f"{val_num:.2f}%"
                                        col_txt_list.append(
                                            f"<b>{pct_str}</b>"
                                            if is_parent
                                            else pct_str
                                        )
                                        targets_map = get_kpi_targets_for_row(
                                            mode, z_ref, pay_type_opt, svc_group_key
                                        )
                                        bg_c, fg_c = evaluate_kpi_cell_style(
                                            val_num,
                                            col_name,
                                            targets_map,
                                            is_parent_row=is_parent,
                                        )
                                        col_fill_list.append(bg_c)
                                        col_font_list.append(fg_c)

                                cell_texts.append(col_txt_list)
                                cell_fills.append(col_fill_list)
                                cell_fonts.append(col_font_list)

                            col_widths = (
                                [1.45, 0.90]
                                + [1.05, 1.05, 1.05]
                                + [0.72] * 8
                                + [0.80]
                            )
                            fig_lt_table = go.Figure(
                                data=[
                                    go.Table(
                                        columnwidth=col_widths,
                                        header=dict(
                                            values=[
                                                f"<b>{h}</b>" for h in table_headers
                                            ],
                                            fill_color="#0f172a",
                                            line_color="#334155",
                                            align=[
                                                "left"
                                                if h == period_col_header
                                                else "center"
                                                for h in table_headers
                                            ],
                                            font=dict(color="#f8fafc", size=11),
                                            height=38,
                                        ),
                                        cells=dict(
                                            values=cell_texts,
                                            fill_color=cell_fills,
                                            line_color="#334155",
                                            align=[
                                                "left"
                                                if h == period_col_header
                                                else "center"
                                                for h in table_headers
                                            ],
                                            font=dict(color=cell_fonts, size=11),
                                            height=28,
                                        ),
                                    )
                                ]
                            )
                            fig_lt_table.update_layout(
                                margin=dict(l=2, r=2, t=8, b=8),
                                paper_bgcolor="rgba(0,0,0,0)",
                                plot_bgcolor="rgba(0,0,0,0)",
                                height=min(
                                    620, max(250, (len(df_matrix_disp) + 1) * 30 + 35)
                                ),
                            )
                            st.plotly_chart(fig_lt_table, use_container_width=True)

                            if curve_rows:
                                df_curve = pd.DataFrame(curve_rows)
                                fig_curve = px.line(
                                    df_curve,
                                    x="Bucket",
                                    y="RunningPct",
                                    color="Zona",
                                    markers=True,
                                    text="RunningPct",
                                    color_discrete_map={
                                        "Zona A": "#38bdf8",
                                        "Zona B": "#28a745",
                                        "Zona C": "#ffc107",
                                    },
                                    labels={
                                        "Bucket": "",
                                        "RunningPct": "% Running in Total",
                                    },
                                )
                                fig_curve.update_traces(
                                    line=dict(width=2.8),
                                    marker=dict(size=7),
                                    texttemplate="%{text:.1f}%",
                                    textposition="top center",
                                    textfont=dict(size=10),
                                )
                                tgt_primary = (
                                    90.0
                                    if (mode == "INBOUND" and pay_type_opt == "COD")
                                    else 95.0
                                )
                                tgt_secondary = (
                                    95.0
                                    if (mode == "INBOUND" and pay_type_opt == "COD")
                                    else (98.0 if mode == "INBOUND" else 99.0)
                                )
                                fig_curve.add_hline(
                                    y=tgt_primary,
                                    line_dash="dash",
                                    line_color="#ffc107",
                                    annotation_text=f"KPI Milestone 1: {tgt_primary:.0f}%",
                                    annotation_font_color="#ffc107",
                                )
                                fig_curve.add_hline(
                                    y=tgt_secondary,
                                    line_dash="dot",
                                    line_color="#28a745",
                                    annotation_text=f"KPI Milestone 2: {tgt_secondary:.0f}%",
                                    annotation_font_color="#28a745",
                                )
                                fig_curve.update_layout(
                                    title=dict(
                                        text="<b>📈 Kurva Kumulatif % Success Delivery per Zona (Pembagi: Total Inbound TRX)</b>",
                                        x=0,
                                        font=dict(size=14, color="#f8fafc"),
                                    ),
                                    xaxis=dict(
                                        categoryorder="array",
                                        categoryarray=LT_RUNNING_BUCKETS,
                                        tickfont=dict(color="#cbd5e1", size=11),
                                    ),
                                    yaxis=dict(
                                        range=[0, 106],
                                        showgrid=True,
                                        gridcolor="rgba(148, 163, 184, 0.15)",
                                        tickfont=dict(color="#cbd5e1"),
                                    ),
                                    legend=dict(
                                        orientation="h",
                                        yanchor="bottom",
                                        y=1.02,
                                        xanchor="right",
                                        x=1,
                                        font=dict(color="#f8fafc"),
                                    ),
                                    plot_bgcolor="rgba(15, 23, 42, 0.7)",
                                    paper_bgcolor="rgba(15, 23, 42, 0.7)",
                                    margin=dict(l=15, r=15, t=50, b=20),
                                    height=330,
                                )
                                st.plotly_chart(fig_curve, use_container_width=True)

                            export_df = df_matrix_disp.drop(
                                columns=["ZonaRef", "IsParent"]
                            ).rename(
                                columns={
                                    "Label": period_col_header,
                                    "VolAWB": "Vol_Inbound_TRX",
                                }
                            )
                            st.download_button(
                                label=f"📥 Download CSV {section_title.split('—')[0].strip()}",
                                data=export_df.to_csv(index=False).encode("utf-8"),
                                file_name=f"SLA_Running_Total_{block_key}_{svc_group_key}.csv",
                                mime="text/csv",
                                key=f"dl_{block_key}",
                            )

                    # 1. Grafik Utama: Latest Inbound -> TGL_UPDATE_STATUS_POD (REG vs JTR/TRC)
                    render_running_total_sla_block(
                        block_key="inbound_delivered",
                        section_title="📊 1. SLA Lead Time: Latest Inbound - Delivered (REG vs JTR/TRC | Pembagi: Inbound TRANSACTION)",
                        mode="INBOUND",
                        base_ts_col="Last_Inbound",
                        bucket_col="lt_inbound_bucket",
                        service_filter_sql="",
                        default_expanded=True,
                    )

                    # 2. Grafik Khusus CTC (Intracity): TGL_ENTRY -> TGL_UPDATE_STATUS_POD
                    render_running_total_sla_block(
                        block_key="ctc_entry_delivered",
                        section_title="📊 2. SLA Lead Time CTC (Intracity): TGL_ENTRY - Delivered (Pembagi: Inbound TRANSACTION)",
                        mode="CTC",
                        base_ts_col="TGL_ENTRY",
                        bucket_col="lt_entry_bucket",
                        service_filter_sql="UPPER(COALESCE(SERVICE, '')) LIKE '%CTC%'",
                        default_expanded=True,
                    )

                    # 3. Grafik Khusus YES: TGL_ENTRY -> TGL_UPDATE_STATUS_POD
                    render_running_total_sla_block(
                        block_key="yes_entry_delivered",
                        section_title="📊 3. SLA Lead Time YES: TGL_ENTRY - Delivered (Pembagi: Inbound TRANSACTION)",
                        mode="YES",
                        base_ts_col="TGL_ENTRY",
                        bucket_col="lt_entry_bucket",
                        service_filter_sql="UPPER(COALESCE(SERVICE, '')) LIKE '%YES%'",
                        default_expanded=True,
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
                            if row_idx == n_rows - 1:
                                col_color_list.append("#0f172a")
                                col_font_list.append(
                                    "#38bdf8" if col == "Grand Total" else "#ffffff"
                                )
                            elif col in ["Cabang", "Grand Total"]:
                                col_color_list.append("#1e293b")
                                col_font_list.append(
                                    "#38bdf8" if col == "Grand Total" else "#ffffff"
                                )
                            else:
                                if val <= 0:
                                    col_color_list.append("#1e293b")
                                    col_font_list.append("#64748b")
                                elif col == "H+0":
                                    col_color_list.append("#28a745")
                                    col_font_list.append("#ffffff")
                                elif col in ["H+1", "H+2"]:
                                    col_color_list.append("#ffc107")
                                    col_font_list.append("#000000")
                                elif col in [
                                    "H+3",
                                    "H+4",
                                    "H+5",
                                    "H+6",
                                    "H+7",
                                    ">H+7",
                                ]:
                                    col_color_list.append("#dc3545")
                                    col_font_list.append("#ffffff")
                                else:
                                    col_color_list.append("#1e293b")
                                    col_font_list.append("#ffffff")
                        cell_colors.append(col_color_list)
                        font_colors.append(col_font_list)

                    st.markdown(
                        '<div class="sla-legend">'
                        '<span><i class="dot-green"></i><b>H+0</b>: Hijau (Fresh / On-Time)</span>'
                        '<span><i class="dot-yellow"></i><b>H+1 – H+2</b>: Kuning (Warning SLA)</span>'
                        '<span><i class="dot-red"></i><b>H+3 – &gt;H+7</b>: Merah (Overdue / Critical)</span>'
                        '<span><i class="dot-zero"></i><b>0</b>: Tidak Ada Backlog</span>'
                        "</div>",
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
                                    align=[
                                        "left" if h == "Cabang" else "center"
                                        for h in headers
                                    ],
                                    font=dict(color="#f8fafc", size=13),
                                    height=34,
                                ),
                                cells=dict(
                                    values=cell_values,
                                    fill_color=cell_colors,
                                    line_color="#334155",
                                    align=[
                                        "left" if h == "Cabang" else "center"
                                        for h in headers
                                    ],
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
                    "📊 4. Matrix Infografis UnRunsheet Per Cabang", expanded=False
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
                    "📊 5. Matrix Infografis Success UnHRS Per Cabang", expanded=False
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
                    "📊 6. Matrix Infografis Undel UnHRS Per Cabang", expanded=False
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
