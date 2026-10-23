import datetime
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
    page_title="Database Inbound 2026", page_icon="🚚", layout="wide"
)
st.title("🚚 Database Inbound 2026")

# =========================================================
# CSS STYLING
# =========================================================
st.markdown(
    """
    <style>
    @keyframes marquee {
        0% { transform: translateX(100%); }
        100% { transform: translateX(-100%); }
    }
    .marquee-container-tab1, .marquee-container-tab2 {
        width: 100%;
        overflow: hidden;
        white-space: nowrap;
        background-color: #f0f2f6;
        padding: 8px 0;
        border-radius: 5px;
        box-sizing: border-box;
        margin-bottom: 15px;
    }
    .marquee-text-tab1, .marquee-text-tab2 {
        display: inline-block;
        padding-left: 75%;
        animation: marquee 50s linear infinite;
        color: #1f77b4;
        font-weight: 600;
        font-size: 14px;
    }
    </style>
""",
    unsafe_allow_html=True,
)

TARGET_GDRIVE_FOLDER_ID = "1qLpozZT-UX2Wm_6F6FzCIjESUkEgbMaP"
OUTPUT_PARQUET_NAME = "data_inbound_2026.parquet"
LOCAL_PARQUET_PATH = os.path.join(os.getcwd(), OUTPUT_PARQUET_NAME)

AGING_ORDER = ["H+0", "H+1", "H+2", "H+3", "H+4", "H+5", "H+6", "H+7", ">H+7"]


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


st.sidebar.header("⚙️ Pengaturan & Sinkronisasi GDrive")
if st.sidebar.button("🔄 Refresh & Tarik Data Terbaru"):
  st.cache_resource.clear()
  st.cache_data.clear()
  if os.path.exists(LOCAL_PARQUET_PATH):
    os.remove(LOCAL_PARQUET_PATH)
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


# =========================================================
# CACHED QUERY FUNCTION (OPTIMIZED DUCKDB MEMORY)
# =========================================================
@st.cache_data(ttl=3600, show_spinner=False)
def run_query(sql_query):
  con = duckdb.connect()
  con.execute("SET memory_limit='512MB';")
  con.execute("SET preserve_insertion_order=false;")
  con.execute(f"""
        CREATE OR REPLACE VIEW v_inbound AS 
        SELECT * FROM read_parquet('{LOCAL_PARQUET_PATH}')
    """)
  df_res = con.execute(sql_query).fetchdf()
  con.close()
  return df_res


try:
  # =========================================================
  # FILTER GLOBAL SIDEBAR
  # =========================================================
  st.sidebar.markdown("---")
  st.sidebar.header("🔍 Filter Data Dashboard")


  @st.cache_data(ttl=3600)
  def get_filter_options():
    con = duckdb.connect()
    con.execute(
        "CREATE VIEW v_inbound AS SELECT * FROM"
        f" read_parquet('{LOCAL_PARQUET_PATH}')"
    )
    locs = (
        con.execute(
            "SELECT DISTINCT LAST_LOCATION FROM v_inbound WHERE LAST_LOCATION IS"
            " NOT NULL ORDER BY 1"
        )
        .fetchdf()["LAST_LOCATION"]
        .tolist()
    )
    cab = (
        con.execute(
            "SELECT DISTINCT Cabang FROM v_inbound WHERE Cabang IS NOT NULL"
            " ORDER BY 1"
        )
        .fetchdf()["Cabang"]
        .tolist()
    )
    zon = (
        con.execute(
            "SELECT DISTINCT Zona FROM v_inbound WHERE Zona IS NOT NULL ORDER"
            " BY 1"
        )
        .fetchdf()["Zona"]
        .tolist()
    )
    kec = (
        con.execute(
            "SELECT DISTINCT Kecamatan FROM v_inbound WHERE Kecamatan IS NOT"
            " NULL ORDER BY 1"
        )
        .fetchdf()["Kecamatan"]
        .tolist()
    )
    stat = (
        con.execute(
            "SELECT DISTINCT Status_Shipment FROM v_inbound WHERE"
            " Status_Shipment IS NOT NULL ORDER BY 1"
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
    minmax = con.execute("""
            SELECT MIN(TRY_STRPTIME(Last_Inbound, '%m/%d/%Y %H:%M')), MAX(TRY_STRPTIME(Last_Inbound, '%m/%d/%Y %H:%M')) 
            FROM v_inbound WHERE Last_Inbound IS NOT NULL AND Last_Inbound != '-'
        """).fetchdf()
    con.close()
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

  default_start = (
      min_max_date.iloc[0, 0].date()
      if not min_max_date.empty and pd.notnull(min_max_date.iloc[0, 0])
      else datetime.date(2026, 1, 1)
  )
  default_end = (
      min_max_date.iloc[0, 1].date()
      if not min_max_date.empty and pd.notnull(min_max_date.iloc[0, 1])
      else datetime.date.today()
  )

  with st.sidebar.form("filter_form"):
    date_range = st.date_input(
        "📅 Filter Rentang Tanggal (Kosongkan untuk Semua Data):",
        value=(),
        min_value=default_start,
        max_value=default_end,
        help="Kosongkan nilai ini jika ingin menampilkan seluruh data termasuk yang tanpa tanggal (UnInbound)",
    )

    chosen_loc = st.multiselect("🔍 Filter Last Location:", unique_locations)
    chosen_cabang = st.multiselect("🏢 Filter Cabang:", unique_cabang)
    chosen_zona = st.multiselect("⚡ Filter Zona (QCE):", unique_zona)
    chosen_kecamatan = st.multiselect(
        "📍 Filter Kecamatan:", unique_kecamatan
    )
    chosen_status = st.multiselect(
        "📦 Filter Shipment Status:", unique_status
    )
    chosen_hrs = st.multiselect("📋 Filter Status HRS:", unique_hrs)

    submit_filter = st.form_submit_button(
        "🚀 Terapkan Filter", use_container_width=True
    )


  def build_in_clause(column_name, selected_list):
    if not selected_list:
      return None
    formatted_items = [
        f"'{str(item).replace('\'', '\'\'')}'" for item in selected_list
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
    where_clauses_general.append(
        build_in_clause("Kecamatan", chosen_kecamatan)
    )
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

  tab1, tab2 = st.tabs(["📊 Main Dashboard", "🔒 Summary Exec"])

  # =========================================================
  # TAB 1: MAIN DASHBOARD
  # =========================================================
  with tab1:
    with st.spinner("⏳ Memuat Main Dashboard..."):
      max_date_df = run_query("""
                SELECT MAX(TRY_STRPTIME(TGL_TARIK_REPORT, '%m/%d/%Y %H:%M')) as max_dt FROM v_inbound
            """)
      max_date = max_date_df["max_dt"][0] if not max_date_df.empty else None
      formatted_date = (
          pd.to_datetime(max_date).strftime("%d %b %Y %H:%M")
          if pd.notnull(max_date)
          else "Waktu pembaruan tidak tersedia"
      )

      top_kec_df = run_query(f"""
                SELECT Kecamatan, COUNT(DISTINCT AWB) as total 
                FROM v_inbound {where_sql} 
                AND Status_Shipment IN ('UnRunsheet', 'UnRunsheet with PRA')
                GROUP BY Kecamatan ORDER BY total DESC LIMIT 5
            """)

      top_kecamatan_str = " &nbsp;&nbsp;&bull;&nbsp;&nbsp; ".join([
          f"{row.Kecamatan} ({row.total:,})"
          for idx, row in top_kec_df.iterrows()
      ])
      if not top_kecamatan_str:
        top_kecamatan_str = "Tidak ada data"

      st.markdown(
          f"""
            <div class="marquee-container-tab1">
                <div class="marquee-text-tab1">
                    🔄 Last Update At: {formatted_date} &nbsp;&nbsp;|&nbsp;&nbsp; 🏆 Top 5 Kecamatan UnRunsheet: {top_kecamatan_str}
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

      metrics_res = run_query(f"""
                SELECT 
                    COUNT(DISTINCT AWB) AS total_awb,
                    COUNT(DISTINCT CASE WHEN Status_Shipment = 'UnInbound' THEN AWB END) AS total_uninbound,
                    COUNT(DISTINCT CASE WHEN Status_Shipment IN ('UnRunsheet', 'UnRunsheet with PRA') THEN AWB END) AS total_unrunsheet,
                    COUNT(DISTINCT CASE WHEN Status_Shipment = 'UnRunsheet with PRA' THEN AWB END) AS total_unrunsheet_pra,
                    COUNT(DISTINCT CASE WHEN Status_Shipment IN ('SUCCESS DELIVERY', 'SUCCESS RETURN SHIPPER') THEN AWB END) AS total_success,
                    COUNT(DISTINCT CASE WHEN Status_Shipment = 'SUCCESS RETURN SHIPPER' THEN AWB END) AS total_return,
                    COUNT(DISTINCT CASE WHEN Status_Shipment IN ('SUCCESS DELIVERY', 'SUCCESS RETURN SHIPPER') AND (HRS = 'UnHRS' OR HRS = '-' OR HRS IS NULL) THEN AWB END) AS total_success_unhrs,
                    COUNT(DISTINCT CASE WHEN Status_Shipment = 'UNDELIVERY' AND (HRS = 'UnHRS' OR HRS = '-' OR HRS IS NULL) THEN AWB END) AS total_undel_unhrs
                FROM v_inbound {where_sql}
            """)

      total_awb = metrics_res["total_awb"][0] or 0
      total_uninbound = metrics_res["total_uninbound"][0] or 0
      total_unrunsheet = metrics_res["total_unrunsheet"][0] or 0
      total_unrunsheet_pra = metrics_res["total_unrunsheet_pra"][0] or 0
      total_success = metrics_res["total_success"][0] or 0
      total_return = metrics_res["total_return"][0] or 0
      total_success_unhrs = metrics_res["total_success_unhrs"][0] or 0
      total_undel_unhrs = metrics_res["total_undel_unhrs"][0] or 0

      # Kalkulasi Persentase
      success_rate = (
          (total_success / total_awb * 100.0) if total_awb > 0 else 0.0
      )
      return_rate = (total_return / total_awb * 100.0) if total_awb > 0 else 0.0
      unrunsheet_rate = (
          (total_unrunsheet / total_awb * 100.0) if total_awb > 0 else 0.0
      )

      # CSS Styling khusus untuk Card Metric di Main Dashboard
      st.markdown(
          """
        <style>
        div[data-testid="stMetric"] {
            background-color: #1e1e1e;
            padding: 12px 16px;
            border-radius: 8px;
            border-left: 4px solid #1f77b4;
            box-shadow: 0 2px 4px rgba(0,0,0,0.2);
            margin-bottom: 8px;
        }
        </style>
      """,
          unsafe_allow_html=True,
      )

      c1, c2, c3, c4 = st.columns(4)
      with c1:
        st.metric(
            label="📦 Total AWB",
            value=f"{total_awb:,}",
            delta=(
                f"Success: {success_rate:.2f}% | Return: {return_rate:.2f}%"
            ),
            delta_color="normal",
        )
      with c2:
        st.metric(label="⚠️ Total UnInbound", value=f"{total_uninbound:,}")
        if st.button(
            "👉 Lihat Data UnInbound",
            key="btn_uninbound",
            use_container_width=True,
        ):
          st.session_state.active_view = "UnInbound"
          st.session_state.active_card_filter = "UnInbound"
          st.rerun()
      with c3:
        st.metric(
            label="🚨 Alert: Total UnRunsheet",
            value=f"{total_unrunsheet:,}",
            delta=f"{unrunsheet_rate:.2f}% dari Total AWB",
            delta_color="inverse",
        )
        if st.button(
            "👉 Lihat Data UnRunsheet",
            key="btn_unrunsheet",
            use_container_width=True,
        ):
          st.session_state.active_view = "UnRunsheet"
          st.session_state.active_card_filter = "Total UnRunsheet"
          st.rerun()
      with c4:
        st.metric(
            label="🚚 UnRunsheet with PRA", value=f"{total_unrunsheet_pra:,}"
        )
        if st.button(
            "👉 Lihat UnRunsheet PRA",
            key="btn_unrunsheet_pra",
            use_container_width=True,
        ):
          st.session_state.active_view = "UnRunsheet PRA"
          st.session_state.active_card_filter = "UnRunsheet with PRA"
          st.rerun()

      col_m1, col_m2 = st.columns(2)
      with col_m1:
        st.metric(label="⚠️ Success UnHRS", value=f"{total_success_unhrs:,}")
        if st.button(
            "📋 Lihat Detail Success UnHRS",
            key="btn_success_unhrs",
            use_container_width=True,
        ):
          st.session_state.active_card_filter = "Success UnHRS"
          st.rerun()
      with col_m2:
        st.metric(label="❌ Undel UnHRS", value=f"{total_undel_unhrs:,}")
        if st.button(
            "📋 Lihat Detail Undel UnHRS",
            key="btn_undel_unhrs",
            use_container_width=True,
        ):
          st.session_state.active_card_filter = "Undel UnHRS"
          st.rerun()

      if st.session_state.active_card_filter:
        card_title = st.session_state.active_card_filter
        with st.expander(
            f"📋 Detail AWB — Berdasarkan Card Filter: [{card_title}]",
            expanded=True,
        ):
          if card_title == "Success UnHRS":
            card_query_filter = (
                "Status_Shipment IN ('SUCCESS DELIVERY', 'SUCCESS RETURN"
                " SHIPPER') AND (HRS = 'UnHRS' OR HRS = '-' OR HRS IS NULL)"
            )
          elif card_title == "Undel UnHRS":
            card_query_filter = (
                "Status_Shipment = 'UNDELIVERY' AND (HRS = 'UnHRS' OR HRS = '-'"
                " OR HRS IS NULL)"
            )
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

          # Tampilan dibatasi 200 baris untuk layar
          df_card_detail_display = run_query(f"""
                        SELECT AWB, Last_Inbound, TGL_UPDATE_STATUS_POD, payment_type, age_days AS "Aging (Hari)", LAST_LOCATION, Cabang, Zona, Kecamatan, SERVICE, Status_Shipment, HRS, RUNSHEET_COURIER_NAME
                        FROM v_inbound {where_sql} AND {card_query_filter}
                        ORDER BY age_days DESC LIMIT 200
                    """)
          # Data penuh untuk download
          df_card_detail_full = run_query(f"""
                        SELECT AWB, Last_Inbound, TGL_UPDATE_STATUS_POD, payment_type, age_days AS "Aging (Hari)", LAST_LOCATION, Cabang, Zona, Kecamatan, SERVICE, Status_Shipment, HRS, RUNSHEET_COURIER_NAME
                        FROM v_inbound {where_sql} AND {card_query_filter}
                        ORDER BY age_days DESC
                    """)
          if not df_card_detail_display.empty:
            st.dataframe(
                df_card_detail_display, use_container_width=True, height=300
            )
            st.caption(
                "ℹ️ Ditampilkan 200 baris teratas untuk performa. Gunakan tombol"
                " di bawah untuk mengunduh seluruh data lengkap."
            )
            c_dl, c_rst = st.columns([3, 1])
            with c_dl:
              st.download_button(
                  f"📥 Download CSV Lengkap [{card_title}]",
                  df_card_detail_full.to_csv(index=False).encode("utf-8"),
                  file_name=f"Detail_{card_title}_Full.csv",
                  mime="text/csv",
              )
            with c_rst:
              if st.button("🔄 Reset Card Filter"):
                st.session_state.active_card_filter = None
                st.rerun()

      st.markdown("---")
      view_choice = st.radio(
          "📌 Pilih Tampilan Data Dashboard:",
          ["📊 Analisis UnRunsheet", "⚠️ Tabel UnInbound"],
          horizontal=True,
      )

      if view_choice == "📊 Analisis UnRunsheet":
        st.subheader("📊 Distribusi Usia Data UnRunsheet")
        unrunsheet_status_clause = (
            ""
            if chosen_status
            else "AND Status_Shipment IN ('UnRunsheet', 'UnRunsheet with PRA')"
        )
        df_aging = run_query(f"""
                    SELECT aging_group, COUNT(DISTINCT AWB) AS total_awb
                    FROM v_inbound {where_sql} {unrunsheet_status_clause}
                    GROUP BY 1 ORDER BY CASE aging_group WHEN 'H+0' THEN 0 WHEN 'H+1' THEN 1 WHEN 'H+2' THEN 2 WHEN 'H+3' THEN 3 WHEN 'H+4' THEN 4 WHEN 'H+5' THEN 5 WHEN 'H+6' THEN 6 WHEN 'H+7' THEN 7 WHEN '>H+7' THEN 8 END
                """)

        st.markdown(
            "📌 **Pilih Filter Kategori Aging (Klik Card untuk Memfilter):**"
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
                f"✨ {ag}\n({vol_ag:,})" if is_selected else f"{ag}\n({vol_ag:,})"
            )
            if st.button(
                btn_label, key=f"card_ag_{ag}", use_container_width=True
            ):
              st.session_state.selected_aging_card = ag
              st.rerun()

        selected_aging_filter = st.session_state.selected_aging_card
        if selected_aging_filter:
          st.info(
              f"📌 Filter Aktif: **Aging = {selected_aging_filter}** (Tabel di"
              " bawah terfilter otomatis)"
          )

        if not df_aging.empty:
          fig = px.bar(
              df_aging,
              x="aging_group",
              y="total_awb",
              text="total_awb",
              labels={"aging_group": "", "total_awb": ""},
          )
          fig.update_layout(
              plot_bgcolor="rgba(40, 40, 40, 0.8)",
              paper_bgcolor="rgba(0, 0, 0, 0)",
              font=dict(color="#CCCCCC"),
              height=330,
          )
          st.plotly_chart(fig, use_container_width=True)

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
          pivot_matrix = run_query(f"""
                        PIVOT (
                            SELECT Status_Shipment, aging_group, AWB 
                            FROM v_inbound {where_sql}
                        )
                        ON aging_group IN ('H+0', 'H+1', 'H+2', 'H+3', 'H+4', 'H+5', 'H+6', 'H+7', '>H+7')
                        USING COUNT(DISTINCT AWB)
                        GROUP BY Status_Shipment
                    """)
          if not pivot_matrix.empty:
            pivot_matrix = pivot_matrix.fillna(0)
            numeric_cols = [
                c for c in pivot_matrix.columns if c != "Status_Shipment"
            ]
            pivot_matrix["Grand Total"] = pivot_matrix[numeric_cols].sum(axis=1)
            st.dataframe(pivot_matrix, use_container_width=True)
          else:
            st.info("Tidak ada data untuk matriks.")

        with st.expander(
            "🗂️ Unrunsheet By Last Location, Cabang, Zona, Kecamatan (Klik"
            " untuk Buka/Tutup)",
            expanded=True,
        ):
          group_level = st.radio(
              "📌 Pilih Level Tampilan (Simulasi Collapse/Expand):",
              [
                  "Detail (Lokasi, Cabang, Zona, Kecamatan)",
                  "Ringkas per Cabang",
                  "Ringkas per Zona",
              ],
              horizontal=True,
          )

          if group_level == "Detail (Lokasi, Cabang, Zona, Kecamatan)":
            group_cols = ["LAST_LOCATION", "Cabang", "Zona", "Kecamatan"]
          elif group_level == "Ringkas per Cabang":
            group_cols = ["Cabang"]
          else:
            group_cols = ["Cabang", "Zona"]

          select_cols_sql = ", ".join(group_cols)

          pivot_loc = run_query(f"""
                        PIVOT (
                            SELECT {select_cols_sql}, aging_group, AWB 
                            FROM v_inbound
                            {where_sql} {unrunsheet_status_clause} {aging_click_clause}
                        )
                        ON aging_group IN ('H+0', 'H+1', 'H+2', 'H+3', 'H+4', 'H+5', 'H+6', 'H+7', '>H+7')
                        USING COUNT(DISTINCT AWB)
                        GROUP BY {select_cols_sql}
                    """)

          if not pivot_loc.empty:
            pivot_loc = pivot_loc.fillna(0)
            for col in AGING_ORDER:
              if col not in pivot_loc.columns:
                pivot_loc[col] = 0

            pivot_loc["Grand Total"] = pivot_loc[AGING_ORDER].sum(axis=1)

            # Fungsi Styler untuk memberikan warna merah pada font H+2 sampai >H+7 jika nilainya > 0
            def highlight_red_alert(val):
              if isinstance(val, (int, float)) and val > 0:
                return "color: #ff4d4d; font-weight: bold;"
              return "color: #888888;"

            red_alert_cols = [
                c
                for c in [
                    "H+2",
                    "H+3",
                    "H+4",
                    "H+5",
                    "H+6",
                    "H+7",
                    ">H+7",
                ]
                if c in pivot_loc.columns
            ]

            styled_pivot = pivot_loc.style.map(
                highlight_red_alert, subset=red_alert_cols
            )

            # Menggunakan st.dataframe dengan Pandas Styler (bebas sort + font merah aktif)
            st.dataframe(styled_pivot, use_container_width=True)

            st.download_button(
                "📥 Download CSV Unrunsheet Hirarki",
                pivot_loc.to_csv(index=False).encode("utf-8"),
                file_name="Unrunsheet_Hirarki_Lokasi.csv",
                mime="text/csv",
            )
          else:
            st.info("Tidak ada data Unrunsheet.")

        with st.expander(
            "📋 Detail AWB (Klik untuk Buka/Tutup)", expanded=False
        ):
          df_detail_display = run_query(f"""
                        SELECT AWB, Last_Inbound, WUS_DATE, payment_type, age_days AS "Aging (Hari)", LAST_LOCATION, Kecamatan, SERVICE, Status_Shipment, HRS, POD_Category, POD_DESC, WUS_REMARKS, RUNSHEET_COURIER_NAME
                        FROM v_inbound {where_sql} {unrunsheet_status_clause} {aging_click_clause}
                        ORDER BY age_days DESC
                        LIMIT 200
                    """)
          df_detail_full = run_query(f"""
                        SELECT AWB, Last_Inbound, WUS_DATE, payment_type, age_days AS "Aging (Hari)", LAST_LOCATION, Kecamatan, SERVICE, Status_Shipment, HRS, POD_Category, POD_DESC, WUS_REMARKS, RUNSHEET_COURIER_NAME
                        FROM v_inbound {where_sql} {unrunsheet_status_clause} {aging_click_clause}
                        ORDER BY age_days DESC
                    """)
          if not df_detail_display.empty:
            st.dataframe(df_detail_display, use_container_width=True)
            st.caption(
                "ℹ️ Ditampilkan 200 baris teratas. Gunakan tombol di bawah"
                " untuk mengunduh seluruh data lengkap."
            )
            st.download_button(
                "📥 Download CSV Lengkap Detail UnRunsheet",
                df_detail_full.to_csv(index=False).encode("utf-8"),
                file_name="Detail_UnRunsheet_Full.csv",
                mime="text/csv",
            )
          else:
            st.info("Tidak ada data detail.")

      else:
        st.subheader("⚠️ Tabel UnInbound")
        df_uninbound_display = run_query(f"""
                    SELECT AWB, Cabang, Zona, Kecamatan, payment_type, age_days AS "Aging from OM/HVO (Hari)", SERVICE, Status_Shipment, HRS
                    FROM v_inbound {where_sql} AND Status_Shipment = 'UnInbound'
                    ORDER BY age_days DESC LIMIT 200
                """)
        df_uninbound_full = run_query(f"""
                    SELECT AWB, Cabang, Zona, Kecamatan, payment_type, age_days AS "Aging from OM/HVO (Hari)", SERVICE, Status_Shipment, HRS
                    FROM v_inbound {where_sql} AND Status_Shipment = 'UnInbound'
                    ORDER BY age_days DESC
                """)
        if not df_uninbound_display.empty:
          st.dataframe(df_uninbound_display, use_container_width=True)
          st.caption(
              "ℹ️ Ditampilkan 200 baris teratas. Gunakan tombol di bawah"
              " untuk mengunduh seluruh data lengkap."
          )
          st.download_button(
              "📥 Download CSV Lengkap UnInbound",
              df_uninbound_full.to_csv(index=False).encode("utf-8"),
              file_name="UnInbound_Full.csv",
              mime="text/csv",
          )

  # =========================================================
  # TAB 2: SUMMARY EXEC
  # =========================================================
  with tab2:
    if summary_pass == "MXG125":
      with st.spinner("⏳ Memuat Executive Summary & Analytics..."):
        st.title(
            "📊 Executive Summary & Performance Analytics"
        )

        avail_weeks_df = run_query(f"""
                    SELECT DISTINCT pod_week_num as wk_num
                    FROM v_inbound {where_sql} AND pod_week_num IS NOT NULL ORDER BY wk_num DESC
                """)
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

        kpi_batch_df = run_query(f"""
                    SELECT 
                        COUNT(DISTINCT CASE WHEN Status_Shipment IN ('SUCCESS DELIVERY', 'SUCCESS RETURN SHIPPER') AND (HRS = 'UnHRS' OR HRS = '-' OR HRS IS NULL) AND UPPER(TRIM(Zona)) IN ('A', 'B') THEN AWB END) AS succ_ab_vol,
                        COUNT(DISTINCT CASE WHEN Status_Shipment IN ('SUCCESS DELIVERY', 'SUCCESS RETURN SHIPPER') AND UPPER(TRIM(Zona)) IN ('A', 'B') THEN AWB END) AS succ_ab_curr_tot,
                        COUNT(DISTINCT CASE WHEN Status_Shipment IN ('SUCCESS DELIVERY', 'SUCCESS RETURN SHIPPER') AND UPPER(TRIM(Zona)) IN ('A', 'B') AND HRS IN ('HRS', 'HOC', 'HRI') AND hrs_aging_group = 'H+0' THEN AWB END) AS succ_ab_curr_tgt,
                        
                        COUNT(DISTINCT CASE WHEN Status_Shipment IN ('SUCCESS DELIVERY', 'SUCCESS RETURN SHIPPER') AND (HRS = 'UnHRS' OR HRS = '-' OR HRS IS NULL) AND UPPER(TRIM(Zona)) = 'C' THEN AWB END) AS succ_c_vol,
                        COUNT(DISTINCT CASE WHEN Status_Shipment IN ('SUCCESS DELIVERY', 'SUCCESS RETURN SHIPPER') AND UPPER(TRIM(Zona)) = 'C' THEN AWB END) AS succ_c_curr_tot,
                        COUNT(DISTINCT CASE WHEN Status_Shipment IN ('SUCCESS DELIVERY', 'SUCCESS RETURN SHIPPER') AND UPPER(TRIM(Zona)) = 'C' AND HRS IN ('HRS', 'HOC', 'HRI') AND hrs_aging_group IN ('H+0', 'H+1') THEN AWB END) AS succ_c_curr_tgt,

                        COUNT(DISTINCT CASE WHEN Status_Shipment = 'UNDELIVERY' AND (HRS = 'UnHRS' OR HRS = '-' OR HRS IS NULL) AND UPPER(TRIM(Zona)) IN ('A', 'B') THEN AWB END) AS undel_ab_vol,
                        COUNT(DISTINCT CASE WHEN Status_Shipment = 'UNDELIVERY' AND UPPER(TRIM(Zona)) IN ('A', 'B') THEN AWB END) AS undel_ab_curr_tot,
                        COUNT(DISTINCT CASE WHEN Status_Shipment = 'UNDELIVERY' AND UPPER(TRIM(Zona)) IN ('A', 'B') AND HRS IN ('HRS', 'HOC', 'HRI') AND hrs_aging_group = 'H+0' THEN AWB END) AS undel_ab_curr_tgt,

                        COUNT(DISTINCT CASE WHEN Status_Shipment = 'UNDELIVERY' AND (HRS = 'UnHRS' OR HRS = '-' OR HRS IS NULL) AND UPPER(TRIM(Zona)) = 'C' THEN AWB END) AS undel_c_vol,
                        COUNT(DISTINCT CASE WHEN Status_Shipment = 'UNDELIVERY' AND UPPER(TRIM(Zona)) = 'C' THEN AWB END) AS undel_c_curr_tot,
                        COUNT(DISTINCT CASE WHEN Status_Shipment = 'UNDELIVERY' AND UPPER(TRIM(Zona)) = 'C' AND HRS IN ('HRS', 'HOC', 'HRI') AND hrs_aging_group IN ('H+0', 'H+1') THEN AWB END) AS undel_c_curr_tgt
                    FROM v_inbound {where_sql} {week_clause}
                """)

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

          def get_badge(title, vol, pct):
            bg = "#28a745" if pct >= 95.0 else "#dc3545"
            return f"""
                        <div style="background-color: #1e1e1e; padding: 14px; border-radius: 8px; border-left: 5px solid {bg}; margin-bottom: 10px;">
                            <div style="font-size: 13px; color: #aaaaaa; font-weight: 600;">{title}</div>
                            <div style="font-size: 24px; font-weight: bold; color: #ffffff; margin: 4px 0;">{vol:,} AWB</div>
                            <div style="font-size: 13px; color: #cccccc;">Capaian Target: <b>{pct:.1f}%</b></div>
                        </div>
                        """

          kc1, kc2, kc3, kc4 = st.columns(4)
          kc1.markdown(
              get_badge(
                  "Success UnHRS (Zona A&B | H+0)",
                  r["succ_ab_vol"] or 0,
                  succ_ab_pct,
              ),
              unsafe_allow_html=True,
          )
          kc2.markdown(
              get_badge(
                  "Success UnHRS (Zona C | ≤ H+1)",
                  r["succ_c_vol"] or 0,
                  succ_c_pct,
              ),
              unsafe_allow_html=True,
          )
          kc3.markdown(
              get_badge(
                  "Undel UnHRS (Zona A&B | H+0)",
                  r["undel_ab_vol"] or 0,
                  undel_ab_pct,
              ),
              unsafe_allow_html=True,
          )
          kc4.markdown(
              get_badge(
                  "Undel UnHRS (Zona C | ≤ H+1)",
                  r["undel_c_vol"] or 0,
                  undel_c_pct,
              ),
              unsafe_allow_html=True,
          )

        st.markdown("---")

        # =========================================================
        # PILIH TAMPILAN GRAFIK & AGREGASI PERIODE (MENGGUNAKAN Last_Inbound)
        # =========================================================
        c_tr_1, c_tr_2 = st.columns([2, 2])
        with c_tr_1:
          st.subheader("📈 Total Inbound Tren (Tahun 2026)")
        with c_tr_2:
          trend_view_mode = st.selectbox(
              "📅 Agregasi Tampilan Grafik:",
              ["Harian (Daily)", "Mingguan (Weekly)", "Bulanan (Monthly)"],
              index=0,
              label_visibility="collapsed",
          )

        # ---------------------------------------------------------
        # WIDGET FILTER RENTANG TANGGAL (DAY RANGE 1-31)
        # ---------------------------------------------------------
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
          if start_day_val > end_day_val:
            st.warning(
                "⚠️ Tanggal awal tidak boleh lebih besar dari tanggal akhir."
            )
            day_filter_clause = ""
          else:
            day_filter_clause = f"AND EXTRACT(DAY FROM TRY_STRPTIME(Last_Inbound, '%m/%d/%Y %H:%M')) BETWEEN {start_day_val} AND {end_day_val}"
        else:
          day_filter_clause = ""

        sql_date_expr = "TRY_STRPTIME(Last_Inbound, '%m/%d/%Y %H:%M')"

        if trend_view_mode == "Harian (Daily)":
          sql_period_expr = f"CAST({sql_date_expr} AS DATE)"
          sql_trend_group = f"""
                        STRFTIME({sql_date_expr}, '%d %b %Y') AS periode,
                        {sql_period_expr} as sort_key
                    """
          group_by_clause = "GROUP BY 1, 2 ORDER BY sort_key ASC"
        elif trend_view_mode == "Mingguan (Weekly)":
          sql_period_expr = f"EXTRACT(WEEK FROM {sql_date_expr})"
          sql_trend_group = f"""
                        CONCAT('Week ', CAST(CAST({sql_period_expr} AS INTEGER) AS STRING)) AS periode,
                        {sql_period_expr} as sort_key
                    """
          group_by_clause = f"""
                        AND {sql_date_expr} IS NOT NULL 
                        GROUP BY 1, 2 ORDER BY sort_key ASC
                    """
        else:
          sql_period_expr = f"STRFTIME({sql_date_expr}, '%Y-%m')"
          sql_trend_group = f"""
                        STRFTIME({sql_date_expr}, '%b %Y') AS periode,
                        {sql_period_expr} as sort_key
                    """
          group_by_clause = "GROUP BY 1, 2 ORDER BY sort_key ASC"

        # Query Trend Inbound menggunakan Last_Inbound sebagai acuan tahun
        df_inbound_trend = run_query(f"""
                    SELECT 
                        {sql_trend_group},
                        COUNT(DISTINCT AWB) AS total_awb
                    FROM v_inbound
                    {where_sql_trend} 
                      AND EXTRACT(YEAR FROM TRY_STRPTIME(Last_Inbound, '%m/%d/%Y %H:%M')) = 2026 {day_filter_clause}
                    {group_by_clause}
                """)

        if not df_inbound_trend.empty:
          fig_inbound = px.line(
              df_inbound_trend,
              x="periode",
              y="total_awb",
              markers=True,
              labels={"periode": "", "total_awb": "Total AWB"},
          )
          fig_inbound.update_layout(
              plot_bgcolor="rgba(15, 32, 67, 0.8)",
              paper_bgcolor="rgba(0, 0, 0, 0)",
              font=dict(color="#CCCCCC"),
              height=330,
          )
          st.plotly_chart(fig_inbound, use_container_width=True)

        # =========================================================
        # TABEL PRODUKTIVITAS PER CABANG (PIVOT MEMANJANG KE KANAN)
        # =========================================================
        st.markdown("---")
        st.subheader("📋 Tabel Produktivitas")

        df_pivot_raw = run_query(f"""
                    SELECT 
                        Cabang,
                        {sql_period_expr} AS period_raw,
                        COUNT(DISTINCT AWB) AS total_awb
                    FROM v_inbound 
                    {where_sql_trend} 
                      AND Cabang IN ('Malang', 'Batu', 'Blitar', 'Kepanjen', 'Bululawang', 'Turen')
                      AND {sql_period_expr} IS NOT NULL
                      AND EXTRACT(YEAR FROM TRY_STRPTIME(Last_Inbound, '%m/%d/%Y %H:%M')) = 2026 {day_filter_clause}
                    GROUP BY Cabang, period_raw
                    ORDER BY period_raw ASC
                """)

        if not df_pivot_raw.empty:
          if trend_view_mode == "Harian (Daily)":
            df_pivot_raw["periode"] = pd.to_datetime(
                df_pivot_raw["period_raw"]
            ).dt.strftime("%d %b %Y")
          elif trend_view_mode == "Mingguan (Weekly)":
            df_pivot_raw["periode"] = (
                "Week " + df_pivot_raw["period_raw"].astype(str)
            )
          else:
            df_pivot_raw["periode"] = pd.to_datetime(
                df_pivot_raw["period_raw"].astype(str) + "-01", errors="coerce"
            ).dt.strftime("%b %Y")
            df_pivot_raw["periode"] = df_pivot_raw["periode"].fillna(
                df_pivot_raw["period_raw"]
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
              match_val = cab_df[cab_df["periode"] == p]["total_awb"].values
              curr_val = int(match_val[0]) if len(match_val) > 0 else 0

              row_dict[f"{p} (AWB)"] = curr_val

              if prev_val is not None and prev_val > 0:
                growth = ((curr_val - prev_val) / prev_val) * 100.0
                row_dict[f"{p} (% Growth)"] = f"{growth:+.2f}%"
              else:
                row_dict[f"{p} (% Growth)"] = "0.00%"

              prev_val = curr_val if curr_val > 0 else prev_val

            pivot_data.append(row_dict)

          df_final_pivot = pd.DataFrame(pivot_data)

          st.dataframe(df_final_pivot, use_container_width=True, height=280)

          st.download_button(
              "📥 Download CSV Produktivitas Cabang (Pivot)",
              df_final_pivot.to_csv(index=False).encode("utf-8"),
              file_name="Produktivitas_Cabang_Pivot.csv",
              mime="text/csv",
          )
        else:
          st.info("Tidak ada data untuk rentang waktu dan cabang tersebut.")
        st.markdown("---")

        def render_plotly_table_matrix(
            df_raw, title_text, csv_filename, download_label
        ):
          if df_raw.empty:
            st.info("Tidak ada data.")
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

          pivot_with_total = pd.concat([pivot, pd.DataFrame([grand_total_row])])
          pivot_with_total = pivot_with_total.reset_index().rename(
              columns={"index": "Cabang"}
          )

          headers = list(pivot_with_total.columns)
          cell_values = []
          for col in headers:
            if col == "Cabang":
              cell_values.append(pivot_with_total[col].astype(str).tolist())
            else:
              cell_values.append(
                  [f"{int(val):,}" for val in pivot_with_total[col].tolist()]
              )

          cell_colors = []
          n_rows = len(pivot_with_total)

          for col in headers:
            col_color_list = []
            for row_idx in range(n_rows):
              if row_idx == n_rows - 1:
                col_color_list.append("#222222")
              elif col in ["Cabang", "Grand Total"]:
                col_color_list.append("#2a2a2a")
              else:
                val = pivot_with_total[col].iloc[row_idx]
                if col == "H+0":
                  col_color_list.append("#28a745" if val > 0 else "#1e1e1e")
                elif col in ["H+1", "H+2"]:
                  col_color_list.append("#ffc107" if val > 0 else "#1e1e1e")
                elif col in ["H+3", "H+4", "H+5", "H+6", "H+7", ">H+7"]:
                  col_color_list.append("#dc3545" if val > 0 else "#1e1e1e")
                else:
                  col_color_list.append("#1e1e1e")
            cell_colors.append(col_color_list)

          font_colors = []
          for col in headers:
            col_font_list = []
            for row_idx in range(n_rows):
              if row_idx == n_rows - 1:
                col_font_list.append("#ffffff")
              elif col in ["H+1", "H+2"]:
                val = pivot_with_total[col].iloc[row_idx]
                col_font_list.append("#000000" if val > 0 else "#ffffff")
              else:
                col_font_list.append("#ffffff")
            font_colors.append(col_font_list)

          fig_table = go.Figure(
              data=[
                  go.Table(
                      header=dict(
                          values=[f"<b>{h}</b>" for h in headers],
                          fill_color="#333333",
                          align="center",
                          font=dict(color="white", size=13),
                          height=32,
                      ),
                      cells=dict(
                          values=cell_values,
                          fill_color=cell_colors,
                          align=[
                              "left" if h == "Cabang" else "center"
                              for h in headers
                          ],
                          font=dict(color=font_colors, size=12),
                          height=28,
                      ),
                  )
              ]
          )

          fig_table.update_layout(
              title=dict(
                  text=f"<b>{title_text}</b>",
                  x=0,
                  xanchor="left",
                  font=dict(size=16),
              ),
              margin=dict(l=10, r=10, t=50, b=10),
              paper_bgcolor="rgba(0,0,0,0)",
              plot_bgcolor="rgba(0,0,0,0)",
              height=320,
          )

          st.plotly_chart(fig_table, use_container_width=True)

          csv_data = pivot_with_total.to_csv(index=False).encode("utf-8")
          st.download_button(
              label=download_label,
              data=csv_data,
              file_name=csv_filename,
              mime="text/csv",
          )

        with st.expander(
            "📊 2. Matrix Infografis UnRunsheet Per Cabang (Klik untuk"
            " Buka/Tutup)",
            expanded=False,
        ):
          df_unrun = run_query(f"""
                        SELECT Cabang, aging_group, COUNT(DISTINCT AWB) AS total_awb 
                        FROM v_inbound
                        {where_sql} AND Status_Shipment IN ('UnRunsheet', 'UnRunsheet with PRA')
                        GROUP BY Cabang, aging_group
                    """)
          render_plotly_table_matrix(
              df_unrun,
              "📊 2. Matrix Infografis UnRunsheet Per Cabang (Aging"
              " Last_Inbound)",
              "Matrix_UnRunsheet_Cabang.csv",
              "📥 Download Data Matrix UnRunsheet (CSV)",
          )

        with st.expander(
            "📊 3. Matrix Infografis Success UnHRS Per Cabang (Klik untuk"
            " Buka/Tutup)",
            expanded=False,
        ):
          df_succ = run_query(f"""
                        SELECT Cabang, wus_aging_group AS aging_group, COUNT(DISTINCT AWB) AS total_awb
                        FROM v_inbound
                        {where_sql} {week_clause} AND Status_Shipment IN ('SUCCESS DELIVERY', 'SUCCESS RETURN SHIPPER')
                          AND (HRS = 'UnHRS' OR HRS = '-' OR HRS IS NULL)
                        GROUP BY Cabang, wus_aging_group
                    """)
          render_plotly_table_matrix(
              df_succ,
              "📊 3. Matrix Infografis Success UnHRS Per Cabang",
              "Matrix_Success_UnHRS_Cabang.csv",
              "📥 Download Data Matrix Success UnHRS (CSV)",
          )

        with st.expander(
            "📊 4. Matrix Infografis Undel UnHRS Per Cabang (Klik untuk"
            " Buka/Tutup)",
            expanded=False,
        ):
          df_undel = run_query(f"""
                        SELECT Cabang, wus_aging_group AS aging_group, COUNT(DISTINCT AWB) AS total_awb
                        FROM v_inbound
                        {where_sql} {week_clause} AND Status_Shipment = 'UNDELIVERY'
                          AND (HRS = 'UnHRS' OR HRS = '-' OR HRS IS NULL)
                        GROUP BY Cabang, wus_aging_group
                    """)
          render_plotly_table_matrix(
              df_undel,
              "📊 4. Matrix Infografis Undel UnHRS Per Cabang",
              "Matrix_Undel_UnHRS_Cabang.csv",
              "📥 Download Data Matrix Undel UnHRS (CSV)",
          )

    else:
      st.warning(
          "🔐 Masukkan password 'Summary Exec' yang benar di sidebar kiri untuk"
          " melihat halaman ini."
      )

except Exception as main_err:
  st.error("⚠️ Terjadi masalah saat mengolah data dashboard:")
  st.exception(main_err)
