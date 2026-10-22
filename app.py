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

try:
  conn = duckdb.connect()
  conn.execute("SET memory_limit='512MB';")

  parquet_columns = conn.execute(
      f"DESCRIBE SELECT * FROM read_parquet('{LOCAL_PARQUET_PATH}')"
  ).fetchdf()["column_name"].tolist()

  wus_col_expr = (
      "WUS_DATE"
      if "WUS_DATE" in parquet_columns
      else "CAST(NULL AS VARCHAR) AS WUS_DATE"
  )
  bil_col_expr = (
      "BILNOTE_AMOUNT"
      if "BILNOTE_AMOUNT" in parquet_columns
      else "CAST(NULL AS DOUBLE) AS BILNOTE_AMOUNT"
  )
  courier_col_expr = (
      "RUNSHEET_COURIER_NAME"
      if "RUNSHEET_COURIER_NAME" in parquet_columns
      else "CAST(NULL AS VARCHAR) AS RUNSHEET_COURIER_NAME"
  )
  tgl_rec_col_expr = (
      "TGL_RECEIVED"
      if "TGL_RECEIVED" in parquet_columns
      else "CAST(NULL AS VARCHAR) AS TGL_RECEIVED"
  )
  hrs_aging_col_expr = (
      "hrs_aging_group"
      if "hrs_aging_group" in parquet_columns
      else "CAST('-' AS VARCHAR) AS hrs_aging_group"
  )
  coding_col_expr = (
      """
        CASE 
            WHEN CODING IS NULL OR CAST(CODING AS VARCHAR) = '' OR CAST(CODING AS VARCHAR) = '-' THEN '-'
            WHEN UPPER(CAST(CODING AS VARCHAR)) LIKE 'D%' THEN UPPER(CAST(CODING AS VARCHAR))
            WHEN TRY_CAST(CODING AS INTEGER) IS NOT NULL THEN 'D' || LPAD(CAST(CODING AS VARCHAR), 2, '0')
            ELSE CAST(CODING AS VARCHAR)
        END AS CODING
    """
      if "CODING" in parquet_columns
      else "CAST('-' AS VARCHAR) AS CODING"
  )

  conn.execute(f"""
        CREATE OR REPLACE VIEW v_inbound AS 
        SELECT 
            *,
            {wus_col_expr},
            {bil_col_expr},
            {courier_col_expr},
            {tgl_rec_col_expr},
            {coding_col_expr},
            {hrs_aging_col_expr},
            CASE 
                WHEN BILNOTE_AMOUNT IS NOT NULL AND CAST(BILNOTE_AMOUNT AS VARCHAR) != '' AND CAST(BILNOTE_AMOUNT AS VARCHAR) != '0' 
                THEN 'COD' 
                ELSE 'Non COD' 
            END AS payment_type
        FROM read_parquet('{LOCAL_PARQUET_PATH}')
    """)

  tab1, tab2 = st.tabs(["📊 Main Dashboard", "🔒 Summary Exec"])

  # =========================================================
  # FILTER GLOBAL SIDEBAR
  # =========================================================
  st.sidebar.markdown("---")
  st.sidebar.header("🔍 Filter Data Dashboard")

  min_max_date = conn.execute("""
        SELECT MIN(TRY_STRPTIME(Last_Inbound, '%m/%d/%Y %H:%M')) as min_d, MAX(TRY_STRPTIME(Last_Inbound, '%m/%d/%Y %H:%M')) as max_d 
        FROM v_inbound WHERE Last_Inbound IS NOT NULL AND Last_Inbound != '-'
    """).fetchdf()

  default_start = (
      min_max_date["min_d"][0].date()
      if not min_max_date.empty and pd.notnull(min_max_date["min_d"][0])
      else datetime.date(2026, 1, 1)
  )
  default_end = (
      min_max_date["max_d"][0].date()
      if not min_max_date.empty and pd.notnull(min_max_date["max_d"][0])
      else datetime.date.today()
  )

  use_date_filter = st.sidebar.checkbox(
      "📅 Aktifkan Filter Tanggal Last Inbound", value=False
  )

  date_range = None
  if use_date_filter:
    date_range = st.sidebar.date_input(
        "Pilih Rentang Tanggal:",
        value=(default_start, default_end),
        min_value=default_start,
        max_value=default_end,
    )

  unique_locations = [
      r[0]
      for r in conn.execute(
          "SELECT DISTINCT LAST_LOCATION FROM v_inbound WHERE LAST_LOCATION IS NOT NULL ORDER BY 1"
      ).fetchall()
  ]

  allowed_cabang_list = [
      "Malang",
      "Batu",
      "Blitar",
      "Kepanjen",
      "Bululawang",
      "Turen",
      "Unknown",
  ]
  unique_cabang_db = [
      r[0]
      for r in conn.execute(
          "SELECT DISTINCT Cabang FROM v_inbound WHERE Cabang IS NOT NULL ORDER BY 1"
      ).fetchall()
  ]
  unique_cabang = [
      c
      for c in unique_cabang_db
      if any(
          allowed.lower() in str(c).lower() for allowed in allowed_cabang_list
      )
  ]

  unique_zona = [
      r[0]
      for r in conn.execute(
          "SELECT DISTINCT Zona FROM v_inbound WHERE Zona IS NOT NULL ORDER BY 1"
      ).fetchall()
  ]
  unique_kecamatan = [
      r[0]
      for r in conn.execute(
          "SELECT DISTINCT Kecamatan FROM v_inbound WHERE Kecamatan IS NOT NULL ORDER BY 1"
      ).fetchall()
  ]
  unique_status = [
      r[0]
      for r in conn.execute(
          "SELECT DISTINCT Status_Shipment FROM v_inbound WHERE Status_Shipment IS NOT NULL ORDER BY 1"
      ).fetchall()
  ]
  unique_hrs = [
      r[0]
      for r in conn.execute(
          "SELECT DISTINCT HRS FROM v_inbound WHERE HRS IS NOT NULL ORDER BY 1"
      ).fetchall()
  ]

  chosen_loc = st.sidebar.multiselect(
      "🔍 Filter Last Location:", unique_locations
  )
  chosen_cabang = st.sidebar.multiselect("🏢 Filter Cabang:", unique_cabang)
  chosen_zona = st.sidebar.multiselect("⚡ Filter Zona (QCE):", unique_zona)
  chosen_kecamatan = st.sidebar.multiselect(
      "📍 Filter Kecamatan:", unique_kecamatan
  )
  chosen_status = st.sidebar.multiselect(
      "📦 Filter Shipment Status:", unique_status
  )
  chosen_hrs = st.sidebar.multiselect("📋 Filter Status HRS:", unique_hrs)

  where_clauses = []
  if use_date_filter and isinstance(date_range, tuple) and len(date_range) == 2:
    start_d, end_d = date_range
    where_clauses.append(
        f"CAST(TRY_STRPTIME(Last_Inbound, '%m/%d/%Y %H:%M') AS DATE) BETWEEN '{start_d}' AND '{end_d}'"
    )

  if chosen_loc:
    loc_str = "','".join(chosen_loc)
    where_clauses.append(f"LAST_LOCATION IN ('{loc_str}')")
  if chosen_cabang:
    cab_str = "','".join(chosen_cabang)
    where_clauses.append(f"Cabang IN ('{cab_str}')")
  if chosen_zona:
    zon_str = "','".join(chosen_zona)
    where_clauses.append(f"Zona IN ('{zon_str}')")
  if chosen_kecamatan:
    kec_str = "','".join(chosen_kecamatan)
    where_clauses.append(f"Kecamatan IN ('{kec_str}')")
  if chosen_status:
    stat_str = "','".join(chosen_status)
    where_clauses.append(f"Status_Shipment IN ('{stat_str}')")
  if chosen_hrs:
    hrs_str = "','".join(chosen_hrs)
    where_clauses.append(f"HRS IN ('{hrs_str}')")

  where_sql = (
      "WHERE " + " AND ".join(where_clauses) if where_clauses else "WHERE 1=1"
  )

  # =========================================================
  # TAB 1: MAIN DASHBOARD
  # =========================================================
  with tab1:
    try:
      max_date_df = conn.execute("""
                SELECT MAX(TRY_STRPTIME(TGL_TARIK_REPORT, '%m/%d/%Y %H:%M')) as max_dt FROM v_inbound
            """).fetchdf()
      max_date = max_date_df["max_dt"][0] if not max_date_df.empty else None
      formatted_date = (
          pd.to_datetime(max_date).strftime("%d %b %Y %H:%M")
          if pd.notnull(max_date)
          else "Waktu pembaruan tidak tersedia"
      )
    except Exception:
      formatted_date = "Format tanggal tidak dikenali"

    top_kec_df = conn.execute(f"""
            SELECT Kecamatan, COUNT(DISTINCT AWB) as total 
            FROM v_inbound {where_sql} 
            AND Status_Shipment IN ('UnRunsheet', 'UnRunsheet with PRA')
            GROUP BY Kecamatan ORDER BY total DESC LIMIT 5
        """).fetchdf()

    top_kecamatan_str = " &nbsp;&nbsp;&bull;&nbsp;&nbsp; ".join([
        f"{row.Kecamatan} ({row.total:,})" for idx, row in top_kec_df.iterrows()
    ])
    if not top_kecamatan_str:
      top_kecamatan_str = "Tidak ada data"

    st.markdown(
        f"""
        <marquee style="color: #1f77b4; background-color: #f0f2f6; padding: 8px; border-radius: 5px; font-weight: 600; font-size: 14px;">
            🔄 Last Update At: {formatted_date} &nbsp;&nbsp;|&nbsp;&nbsp; 🏆 Top 5 Kecamatan UnRunsheet: {top_kecamatan_str}
        </marquee>
    """,
        unsafe_allow_html=True,
    )

    if "active_view" not in st.session_state:
      st.session_state.active_view = "UnRunsheet"
    if "active_card_filter" not in st.session_state:
      st.session_state.active_card_filter = None

    metrics_res = conn.execute(f"""
            SELECT 
                COUNT(DISTINCT AWB) AS total_awb,
                COUNT(DISTINCT CASE WHEN Status_Shipment = 'UnInbound' THEN AWB END) AS total_uninbound,
                COUNT(DISTINCT CASE WHEN Status_Shipment IN ('UnRunsheet', 'UnRunsheet with PRA') THEN AWB END) AS total_unrunsheet,
                COUNT(DISTINCT CASE WHEN Status_Shipment = 'UnRunsheet with PRA' THEN AWB END) AS total_unrunsheet_pra
            FROM v_inbound {where_sql}
        """).fetchdf()

    total_awb = metrics_res["total_awb"][0] or 0
    total_uninbound = metrics_res["total_uninbound"][0] or 0
    total_unrunsheet = metrics_res["total_unrunsheet"][0] or 0
    total_unrunsheet_pra = metrics_res["total_unrunsheet_pra"][0] or 0

    c1, c2, c3, c4 = st.columns(4)
    with c1:
      st.metric(label="📦 Total AWB", value=f"{total_awb:,}")
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
          label="🚨 Alert: Total UnRunsheet", value=f"{total_unrunsheet:,}"
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

    st.markdown("---")

    metrics_custom = conn.execute(f"""
            SELECT 
                COUNT(DISTINCT CASE 
                    WHEN Status_Shipment IN ('SUCCESS DELIVERY', 'SUCCESS RETURN SHIPPER') 
                         AND (HRS = 'UnHRS' OR HRS = '-' OR HRS IS NULL) 
                    THEN AWB END) AS total_success_unhrs,
                COUNT(DISTINCT CASE 
                    WHEN Status_Shipment = 'UNDELIVERY' 
                         AND (HRS = 'UnHRS' OR HRS = '-' OR HRS IS NULL) 
                    THEN AWB END) AS total_undel_unhrs
            FROM v_inbound {where_sql}
        """).fetchdf()

    total_success_unhrs = metrics_custom["total_success_unhrs"][0] or 0
    total_undel_unhrs = metrics_custom["total_undel_unhrs"][0] or 0

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
      if st.button(
          f"🔄 Reset Filter Card [{st.session_state.active_card_filter}]"
      ):
        st.session_state.active_card_filter = None
        st.rerun()

    st.markdown("---")

    if st.session_state.active_card_filter:
      card_title = st.session_state.active_card_filter
      st.subheader(
          f"📋 Tabel Detail AWB — Berdasarkan Klik Card: [{card_title}]"
      )

      if card_title == "Success UnHRS":
        card_query_filter = """
                    Status_Shipment IN ('SUCCESS DELIVERY', 'SUCCESS RETURN SHIPPER') 
                    AND (HRS = 'UnHRS' OR HRS = '-' OR HRS IS NULL)
                """
        df_card_detail = conn.execute(f"""
                    SELECT AWB, LAST_LOCATION AS Last_Location, TGL_UPDATE_STATUS_POD, CODING, payment_type AS "Kategori COD / Non-COD", BILNOTE_AMOUNT, RUNSHEET_COURIER_NAME
                    FROM v_inbound
                    {where_sql} AND {card_query_filter}
                    LIMIT 1000
                """).fetchdf()
      else:
        if card_title == "Undel UnHRS":
          card_query_filter = """
                        Status_Shipment = 'UNDELIVERY' 
                        AND (HRS = 'UnHRS' OR HRS = '-' OR HRS IS NULL)
                    """
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

        df_card_detail = conn.execute(f"""
                    SELECT AWB, Last_Inbound, TGL_UPDATE_STATUS_POD, payment_type, age_days AS "Aging Last Inbound (Hari)", LAST_LOCATION, Cabang, Zona, Kecamatan, SERVICE, Status_Shipment, HRS, POD_Category, POD_DESC, WUS_REMARKS, RUNSHEET_COURIER_NAME
                    FROM v_inbound
                    {where_sql} AND {card_query_filter}
                    ORDER BY age_days DESC
                    LIMIT 1000
                """).fetchdf()

      if not df_card_detail.empty:
        st.caption(
            f"Total AWB Ditampilkan: **{len(df_card_detail):,}** (Maksimal 1.000"
            " baris)"
        )
        st.dataframe(df_card_detail, use_container_width=True, height=400)

        csv_card = df_card_detail.to_csv(index=False).encode("utf-8")
        st.download_button(
            label=f"📥 Download Data Detail [{card_title}] (CSV)",
            data=csv_card,
            file_name=f"Detail_Card_{card_title.replace(' ', '_')}.csv",
            mime="text/csv",
        )
      else:
        st.info(
            f"Tidak ada data AWB yang cocok untuk kategori [{card_title}]"
            " dengan filter aktif."
        )

      st.markdown("---")

    view_choice = st.radio(
        "📌 Pilih Tampilan Data Dashboard:",
        ["📊 Analisis UnRunsheet", "⚠️ Tabel UnInbound"],
        horizontal=True,
        index=1 if st.session_state.active_view == "UnInbound" else 0,
    )

    if view_choice == "📊 Analisis UnRunsheet":
      st.subheader(
          "📊 Distribusi Usia Data UnRunsheet (Aging dari Last_Inbound ke Hari"
          " Ini)"
      )

      unrunsheet_status_clause = (
          ""
          if chosen_status
          else "AND Status_Shipment IN ('UnRunsheet', 'UnRunsheet with PRA')"
      )

      df_aging = conn.execute(f"""
                SELECT aging_group, COUNT(DISTINCT AWB) AS total_awb
                FROM v_inbound
                {where_sql} {unrunsheet_status_clause}
                GROUP BY 1
                ORDER BY 
                    CASE aging_group
                        WHEN 'H+0' THEN 0 WHEN 'H+1' THEN 1 WHEN 'H+2' THEN 2 WHEN 'H+3' THEN 3 
                        WHEN 'H+4' THEN 4 WHEN 'H+5' THEN 5 WHEN 'H+6' THEN 6 WHEN 'H+7' THEN 7 WHEN '>H+7' THEN 8
                    END
            """).fetchdf()

      selected_aging_group = "Semua Aging"
      col_chart, col_table = st.columns([2, 1])

      with col_chart:
        if not df_aging.empty:
          fig = px.bar(
              df_aging,
              x="aging_group",
              y="total_awb",
              text="total_awb",
              labels={"aging_group": "", "total_awb": ""},
          )
          fig.update_traces(
              textposition="outside",
              marker_color="rgba(30, 110, 220, 0.4)",
              marker_line_color="rgb(60, 150, 255)",
              marker_line_width=2,
              width=0.3,
          )
          fig.update_layout(
              plot_bgcolor="rgba(40, 40, 40, 0.8)",
              paper_bgcolor="rgba(0, 0, 0, 0)",
              font=dict(color="#CCCCCC", size=13),
              xaxis=dict(
                  type="category",
                  tickangle=0,
                  gridcolor="rgba(255, 255, 255, 0.05)",
                  categoryorder="array",
                  categoryarray=AGING_ORDER,
              ),
              yaxis=dict(gridcolor="rgba(255, 255, 255, 0.08)"),
              margin=dict(l=20, r=20, t=30, b=20),
              height=380,
          )
          st.plotly_chart(fig, use_container_width=True)

      with col_table:
        st.markdown("**Klik / Pilih Baris Tabel di Bawah:**")
        if not df_aging.empty:
          event = st.dataframe(
              df_aging,
              use_container_width=True,
              on_select="rerun",
              selection_mode="single-row",
          )
          if event.selection.get("rows", []):
            selected_aging_group = df_aging.iloc[event.selection["rows"][0]][
                "aging_group"
            ]
            st.success(f"🎯 Filter Aktif: **{selected_aging_group}**")

      aging_clause = (
          f"AND aging_group = '{selected_aging_group}'"
          if selected_aging_group != "Semua Aging"
          else ""
      )

      st.markdown("---")

      st.subheader(
          "📊 Tabel Matrix Status Shipment (Filter Aging:"
          f" [{selected_aging_group}])"
      )

      pivot_matrix = conn.execute(f"""
                PIVOT (
                    SELECT Status_Shipment, aging_group, AWB 
                    FROM v_inbound {where_sql} {aging_clause}
                )
                ON aging_group IN ('H+0', 'H+1', 'H+2', 'H+3', 'H+4', 'H+5', 'H+6', 'H+7', '>H+7')
                USING COUNT(DISTINCT AWB)
                GROUP BY Status_Shipment
            """).fetchdf()

      if not pivot_matrix.empty:
        pivot_matrix = pivot_matrix.fillna(0)
        numeric_cols = [
            c for c in pivot_matrix.columns if c != "Status_Shipment"
        ]
        pivot_matrix["Grand Total"] = pivot_matrix[numeric_cols].sum(axis=1)
        st.dataframe(pivot_matrix, use_container_width=True)
      else:
        st.info("Tidak ada data untuk kombinasi filter yang dipilih.")

      st.markdown("---")

      st.subheader(
          "🗂️ Unrunsheet By Last Location (Filter Aging:"
          f" [{selected_aging_group}])"
      )

      pivot_loc = conn.execute(f"""
                PIVOT (
                    SELECT LAST_LOCATION, Cabang, Zona, Kecamatan, aging_group, AWB 
                    FROM v_inbound
                    {where_sql} {unrunsheet_status_clause} {aging_clause}
                )
                ON aging_group IN ('H+0', 'H+1', 'H+2', 'H+3', 'H+4', 'H+5', 'H+6', 'H+7', '>H+7')
                USING COUNT(DISTINCT AWB)
                GROUP BY LAST_LOCATION, Cabang, Zona, Kecamatan
            """).fetchdf()

      if not pivot_loc.empty:
        pivot_loc = pivot_loc.fillna(0)
        num_cols = [
            c
            for c in pivot_loc.columns
            if c not in ["LAST_LOCATION", "Cabang", "Zona", "Kecamatan"]
        ]
        pivot_loc["Grand Total"] = pivot_loc[num_cols].sum(axis=1)
        st.dataframe(pivot_loc, use_container_width=True)
      else:
        st.info("Tidak ada data Unrunsheet.")

      st.subheader(
          f"📋 Detail AWB — Kategori Aging: [{selected_aging_group}]"
      )

      df_detail = conn.execute(f"""
                SELECT AWB, Last_Inbound, WUS_DATE, payment_type, age_days AS "Aging Last Inbound (Hari)", LAST_LOCATION, Kecamatan, SERVICE, Status_Shipment, HRS, POD_Category, POD_DESC, WUS_REMARKS, RUNSHEET_COURIER_NAME
                FROM v_inbound
                {where_sql} {unrunsheet_status_clause} {aging_clause}
                ORDER BY age_days DESC
                LIMIT 1000
            """).fetchdf()

      st.dataframe(df_detail, use_container_width=True)

    else:
      st.subheader(
          "⚠️ Tabel UnInbound (Last Inbound Kosong & Tanpa Runsheet MXG)"
      )
      df_uninbound_show = conn.execute(f"""
                SELECT AWB, Cabang, Zona, Kecamatan, payment_type, age_days AS "Aging Last Inbound (Hari)", SERVICE, Status_Shipment, HRS
                FROM v_inbound
                {where_sql} AND Status_Shipment = 'UnInbound'
                ORDER BY age_days DESC
                LIMIT 1000
            """).fetchdf()

      if not df_uninbound_show.empty:
        st.dataframe(df_uninbound_show, use_container_width=True)

        csv_u = df_uninbound_show.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="📥 Download Data CSV UnInbound",
            data=csv_u,
            file_name="Data_UnInbound.csv",
            mime="text/csv",
        )
      else:
        st.info("Tidak ada data UnInbound.")

  # =========================================================
  # TAB 2: SUMMARY EXEC (KPI & WUS vs POD AGING PERFORMANCE)
  # =========================================================
  with tab2:
    if summary_pass == "MXG125":
      st.title(
          "📊 Executive Summary & Performance Analytics (WUS vs POD Validation)"
      )

      def get_kpi_metrics(
          status_filter_sql,
          unhrs_filter_sql,
          zona_filter_sql,
          target_aging_condition_sql,
      ):
        tot_all_df = conn.execute(f"""
                    SELECT COUNT(DISTINCT AWB) as total_awb
                    FROM v_inbound {where_sql}
                    AND {status_filter_sql}
                    AND {unhrs_filter_sql}
                    AND {zona_filter_sql}
                """).fetchdf()

        tot_all_value = tot_all_df["total_awb"][0] or 0

        week_info = conn.execute(f"""
                    SELECT WEEK(TRY_STRPTIME(TGL_UPDATE_STATUS_POD, '%m/%d/%Y %H:%M')) as wk_num
                    FROM v_inbound {where_sql}
                    AND {status_filter_sql}
                    AND {zona_filter_sql}
                    AND TGL_UPDATE_STATUS_POD IS NOT NULL AND TGL_UPDATE_STATUS_POD != '-'
                    ORDER BY TRY_STRPTIME(TGL_UPDATE_STATUS_POD, '%m/%d/%Y %H:%M') DESC
                    LIMIT 1
                """).fetchdf()

        current_week = (
            week_info["wk_num"][0]
            if not week_info.empty and pd.notnull(week_info["wk_num"][0])
            else 43
        )
        prev_week = current_week - 1 if current_week > 0 else 0

        curr_df = conn.execute(f"""
                    SELECT 
                        COUNT(DISTINCT AWB) as total_kiriman,
                        COUNT(DISTINCT CASE WHEN HRS IN ('HRS', 'HOC', 'HRI') AND ({target_aging_condition_sql}) THEN AWB END) as total_hrs_target
                    FROM v_inbound {where_sql}
                    AND {status_filter_sql}
                    AND {zona_filter_sql}
                    AND WEEK(TRY_STRPTIME(TGL_UPDATE_STATUS_POD, '%m/%d/%Y %H:%M')) = {current_week}
                """).fetchdf()

        curr_tot = curr_df["total_kiriman"][0] or 0
        curr_tgt = curr_df["total_hrs_target"][0] or 0
        curr_pct = (curr_tgt / curr_tot * 100.0) if curr_tot > 0 else 0.0

        prev_df = conn.execute(f"""
                    SELECT 
                        COUNT(DISTINCT AWB) as total_kiriman,
                        COUNT(DISTINCT CASE WHEN HRS IN ('HRS', 'HOC', 'HRI') AND ({target_aging_condition_sql}) THEN AWB END) as total_hrs_target
                    FROM v_inbound {where_sql}
                    AND {status_filter_sql}
                    AND {zona_filter_sql}
                    AND WEEK(TRY_STRPTIME(TGL_UPDATE_STATUS_POD, '%m/%d/%Y %H:%M')) = {prev_week}
                """).fetchdf()

        prev_tot = prev_df["total_kiriman"][0] or 0
        prev_tgt = prev_df["total_hrs_target"][0] or 0
        prev_pct = (prev_tgt / prev_tot * 100.0) if prev_tot > 0 else 0.0

        wow_diff = curr_pct - prev_pct
        return tot_all_value, curr_pct, wow_diff, current_week, prev_week

      succ_ab_vol, succ_ab_pct, succ_ab_wow, cur_w, prv_w = get_kpi_metrics(
          status_filter_sql=(
              "Status_Shipment IN ('SUCCESS DELIVERY', 'SUCCESS RETURN SHIPPER')"
          ),
          unhrs_filter_sql="(HRS = 'UnHRS' OR HRS = '-' OR HRS IS NULL)",
          zona_filter_sql="UPPER(TRIM(Zona)) IN ('A', 'B')",
          target_aging_condition_sql="hrs_aging_group = 'H+0'",
      )

      succ_c_vol, succ_c_pct, succ_c_wow, _, _ = get_kpi_metrics(
          status_filter_sql=(
              "Status_Shipment IN ('SUCCESS DELIVERY', 'SUCCESS RETURN SHIPPER')"
          ),
          unhrs_filter_sql="(HRS = 'UnHRS' OR HRS = '-' OR HRS IS NULL)",
          zona_filter_sql="UPPER(TRIM(Zona)) = 'C'",
          target_aging_condition_sql="hrs_aging_group IN ('H+0', 'H+1')",
      )

      undel_ab_vol, undel_ab_pct, undel_ab_wow, _, _ = get_kpi_metrics(
          status_filter_sql="Status_Shipment = 'UNDELIVERY'",
          unhrs_filter_sql="(HRS = 'UnHRS' OR HRS = '-' OR HRS IS NULL)",
          zona_filter_sql="UPPER(TRIM(Zona)) IN ('A', 'B')",
          target_aging_condition_sql="hrs_aging_group = 'H+0'",
      )

      undel_c_vol, undel_c_pct, undel_c_wow, _, _ = get_kpi_metrics(
          status_filter_sql="Status_Shipment = 'UNDELIVERY'",
          unhrs_filter_sql="(HRS = 'UnHRS' OR HRS = '-' OR HRS IS NULL)",
          zona_filter_sql="UPPER(TRIM(Zona)) = 'C'",
          target_aging_condition_sql="hrs_aging_group IN ('H+0', 'H+1')",
      )

      st.caption(
          f"📌 Perbandingan Performansi WUS: **Week {cur_w}** vs **Week"
          f" {prv_w}**"
      )

      def get_status_alert_badge(title, pct_val, wow_val):
        status_icon = "✅" if pct_val >= 95.0 else "🚨"
        status_text = (
            "Target Achieved" if pct_val >= 95.0 else "Below Target (<95%)"
        )
        badge_color = "#28a745" if pct_val >= 95.0 else "#dc3545"

        wow_color = "#28a745" if wow_val >= 0 else "#dc3545"
        wow_sign = "+" if wow_val >= 0 else ""

        return f"""
        <div style="background-color: #1e1e1e; padding: 12px; border-radius: 8px; border-left: 5px solid {badge_color}; margin-bottom: 10px;">
            <div style="font-size: 13px; color: #aaaaaa; font-weight: 600;">{status_icon} {title}</div>
            <div style="font-size: 11px; color: {badge_color}; font-weight: bold; margin-top: 2px;">{status_text}</div>
            <div style="font-size: 24px; font-weight: bold; color: #ffffff; margin: 4px 0;">{pct_val:.1f}%</div>
            <div style="font-size: 12px; color: #cccccc;">
                Capaian <span style="color: {wow_color}; font-weight: bold;">({wow_sign}{wow_val:.1f}% WoW)</span>
            </div>
        </div>
        """

      kc1, kc2, kc3, kc4 = st.columns(4)
      with kc1:
        st.markdown(
            get_status_alert_badge(
                "Success UnHRS (Zona A&B | H+0)", succ_ab_pct, succ_ab_wow
            ),
            unsafe_allow_html=True,
        )
        st.caption(f"📦 Total UnHRS: **{succ_ab_vol:,} AWB**")

      with kc2:
        st.markdown(
            get_status_alert_badge(
                "Success UnHRS (Zona C | ≤ H+1)", succ_c_pct, succ_c_wow
            ),
            unsafe_allow_html=True,
        )
        st.caption(f"📦 Total UnHRS: **{succ_c_vol:,} AWB**")

      with kc3:
        st.markdown(
            get_status_alert_badge(
                "Undel UnHRS (Zona A&B | H+0)", undel_ab_pct, undel_ab_wow
            ),
            unsafe_allow_html=True,
        )
        st.caption(f"📦 Total UnHRS: **{undel_ab_vol:,} AWB**")

      with kc4:
        st.markdown(
            get_status_alert_badge(
                "Undel UnHRS (Zona C | ≤ H+1)", undel_c_pct, undel_c_wow
            ),
            unsafe_allow_html=True,
        )
        st.caption(f"📦 Total UnHRS: **{undel_c_vol:,} AWB**")

      st.markdown("---")

      if "selected_hrs_category" not in st.session_state:
        st.session_state.selected_hrs_category = None

      st.subheader("🔍 Detail Capaian HRS (Running Total Persentase Aging)")
      st.caption(
          "Pilih tombol kategori di bawah untuk menampilkan matriks persentase"
          " running total H+0 s/d >H+7 per Last Location:"
      )

      b1, b2, b3, b4 = st.columns(4)
      with b1:
        if st.button(
            "📋 Detail Success (Zona A&B)",
            use_container_width=True,
            key="btn_det_succ_ab",
        ):
          st.session_state.selected_hrs_category = "Success Zona A&B"
      with b2:
        if st.button(
            "📋 Detail Success (Zona C)",
            use_container_width=True,
            key="btn_det_succ_c",
        ):
          st.session_state.selected_hrs_category = "Success Zona C"
      with b3:
        if st.button(
            "📋 Detail Undel (Zona A&B)",
            use_container_width=True,
            key="btn_det_undel_ab",
        ):
          st.session_state.selected_hrs_category = "Undel Zona A&B"
      with b4:
        if st.button(
            "📋 Detail Undel (Zona C)",
            use_container_width=True,
            key="btn_det_undel_c",
        ):
          st.session_state.selected_hrs_category = "Undel Zona C"

      if st.session_state.selected_hrs_category:
        hrs_cat = st.session_state.selected_hrs_category
        st.markdown(f"#### 📊 Matriks Running Total HRS: [{hrs_cat}]")

        if hrs_cat == "Success Zona A&B":
          hrs_status_sql = (
              "Status_Shipment IN ('SUCCESS DELIVERY', 'SUCCESS RETURN SHIPPER')"
          )
          hrs_zona_sql = "UPPER(TRIM(Zona)) IN ('A', 'B')"
        elif hrs_cat == "Success Zona C":
          hrs_status_sql = (
              "Status_Shipment IN ('SUCCESS DELIVERY', 'SUCCESS RETURN SHIPPER')"
          )
          hrs_zona_sql = "UPPER(TRIM(Zona)) = 'C'"
        elif hrs_cat == "Undel Zona A&B":
          hrs_status_sql = "Status_Shipment = 'UNDELIVERY'"
          hrs_zona_sql = "UPPER(TRIM(Zona)) IN ('A', 'B')"
        else:
          hrs_status_sql = "Status_Shipment = 'UNDELIVERY'"
          hrs_zona_sql = "UPPER(TRIM(Zona)) = 'C'"

        # Query Sangat Cepat Langsung Membaca Parquet
        df_running_hrs = conn.execute(f"""
                    WITH base AS (
                        SELECT 
                            LAST_LOCATION,
                            COUNT(DISTINCT AWB) as total_all,
                            COUNT(DISTINCT CASE WHEN hrs_aging_group = 'H+0' THEN AWB END) as h0,
                            COUNT(DISTINCT CASE WHEN hrs_aging_group IN ('H+0', 'H+1') THEN AWB END) as h1,
                            COUNT(DISTINCT CASE WHEN hrs_aging_group IN ('H+0', 'H+1', 'H+2') THEN AWB END) as h2,
                            COUNT(DISTINCT CASE WHEN hrs_aging_group IN ('H+0', 'H+1', 'H+2', 'H+3') THEN AWB END) as h3,
                            COUNT(DISTINCT CASE WHEN hrs_aging_group IN ('H+0', 'H+1', 'H+2', 'H+3', 'H+4') THEN AWB END) as h4,
                            COUNT(DISTINCT CASE WHEN hrs_aging_group IN ('H+0', 'H+1', 'H+2', 'H+3', 'H+4', 'H+5') THEN AWB END) as h5,
                            COUNT(DISTINCT CASE WHEN hrs_aging_group IN ('H+0', 'H+1', 'H+2', 'H+3', 'H+4', 'H+5', 'H+6') THEN AWB END) as h6,
                            COUNT(DISTINCT CASE WHEN hrs_aging_group IN ('H+0', 'H+1', 'H+2', 'H+3', 'H+4', 'H+5', 'H+6', 'H+7') THEN AWB END) as h7,
                            COUNT(DISTINCT CASE WHEN hrs_aging_group IS NOT NULL AND hrs_aging_group != '-' THEN AWB END) as h_gt
                        FROM v_inbound {where_sql}
                        AND {hrs_status_sql}
                        AND {hrs_zona_sql}
                        GROUP BY LAST_LOCATION
                    )
                    SELECT 
                        LAST_LOCATION,
                        ROUND(h0 * 100.0 / NULLIF(total_all, 0), 2) AS "H+0",
                        ROUND(h1 * 100.0 / NULLIF(total_all, 0), 2) AS "H+1",
                        ROUND(h2 * 100.0 / NULLIF(total_all, 0), 2) AS "H+2",
                        ROUND(h3 * 100.0 / NULLIF(total_all, 0), 2) AS "H+3",
                        ROUND(h4 * 100.0 / NULLIF(total_all, 0), 2) AS "H+4",
                        ROUND(h5 * 100.0 / NULLIF(total_all, 0), 2) AS "H+5",
                        ROUND(h6 * 100.0 / NULLIF(total_all, 0), 2) AS "H+6",
                        ROUND(h7 * 100.0 / NULLIF(total_all, 0), 2) AS "H+7",
                        ROUND(h_gt * 100.0 / NULLIF(total_all, 0), 2) AS ">H+7"
                    FROM base
                    ORDER BY LAST_LOCATION ASC
                """).fetchdf()

        if not df_running_hrs.empty:
          pct_cols = [
              "H+0",
              "H+1",
              "H+2",
              "H+3",
              "H+4",
              "H+5",
              "H+6",
              "H+7",
              ">H+7",
          ]
          column_config = {
              c: st.column_config.NumberColumn(
                  label=c, format="%.2f%%", min_value=0, max_value=100
              )
              for c in pct_cols
          }

          st.dataframe(
              df_running_hrs,
              column_config=column_config,
              use_container_width=True,
          )

          col_dl, col_cls = st.columns([3, 1])
          with col_dl:
            csv_run = df_running_hrs.to_csv(index=False).encode("utf-8")
            st.download_button(
                label=f"📥 Download Data Matrix Running Total [{hrs_cat}] (CSV)",
                data=csv_run,
                file_name=f"Matrix_Running_Total_{hrs_cat.replace(' ', '_')}.csv",
                mime="text/csv",
            )
          with col_cls:
            if st.button("❌ Tutup Tabel Detail"):
              st.session_state.selected_hrs_category = None
              st.rerun()
        else:
          st.info(
              f"Tidak ada data untuk kategori [{hrs_cat}] dengan filter yang"
              " aktif."
          )

      st.markdown("---")

      cab_counts = conn.execute(f"""
                SELECT Cabang, COUNT(DISTINCT AWB) as total
                FROM v_inbound
                {where_sql}
                GROUP BY Cabang
            """).fetchdf()

      summary_str = (
          " &nbsp;&nbsp;&bull;&nbsp;&nbsp; ".join([
              f"<b>{row.Cabang}</b>: {row.total:,} AWB"
              for idx, row in cab_counts.iterrows()
          ])
          if not cab_counts.empty
          else "Tidak ada data sesuai filter"
      )

      st.markdown(
          f"""
            <marquee style="color: #2c3e50; background-color: #e8f4f8; padding: 10px; border-radius: 5px; font-weight: 600; font-size: 15px;">
                🔄 Last Update: {formatted_date} &nbsp;&nbsp;|&nbsp;&nbsp; 🏢 <b>Total Inbound (Filtered):</b> {summary_str}
            </marquee>
        """,
          unsafe_allow_html=True,
      )

      st.markdown("---")

      st.subheader("📈 1. Grafik Total Inbound Per Tanggal (Tahun 2026)")

      col_gran, col_date = st.columns([1, 1])

      with col_gran:
        granularity = st.radio(
            "Pilih Granularitas Waktu:",
            ["Hari", "Week (Minggu)", "Bulan"],
            horizontal=True,
            key="exec_granularity",
        )

      min_max_inbound_2026 = conn.execute(f"""
                SELECT 
                    MIN(CAST(COALESCE(dt_im, CASE WHEN UPPER(COALESCE(HVI_NO, '')) LIKE '%MXG%' THEN dt_hvi ELSE NULL END) AS DATE)) as min_d, 
                    MAX(CAST(COALESCE(dt_im, CASE WHEN UPPER(COALESCE(HVI_NO, '')) LIKE '%MXG%' THEN dt_hvi ELSE NULL END) AS DATE)) as max_d 
                FROM v_inbound 
                {where_sql} AND EXTRACT(YEAR FROM COALESCE(dt_im, CASE WHEN UPPER(COALESCE(HVI_NO, '')) LIKE '%MXG%' THEN dt_hvi ELSE NULL END)) = 2026
            """).fetchdf()

      inbound_start = (
          min_max_inbound_2026["min_d"][0]
          if not min_max_inbound_2026.empty
          and pd.notnull(min_max_inbound_2026["min_d"][0])
          else datetime.date(2026, 1, 1)
      )
      inbound_end = (
          min_max_inbound_2026["max_d"][0]
          if not min_max_inbound_2026.empty
          and pd.notnull(min_max_inbound_2026["max_d"][0])
          else datetime.date(2026, 12, 31)
      )

      with col_date:
        inbound_date_range = st.date_input(
            "📅 Filter Rentang Tanggal (Inbound 2026):",
            value=(inbound_start, inbound_end),
            min_value=datetime.date(2026, 1, 1),
            max_value=datetime.date(2026, 12, 31),
            key="exec_date_range",
        )

      inbound_date_clause = ""
      if (
          isinstance(inbound_date_range, tuple)
          and len(inbound_date_range) == 2
      ):
        s_date, e_date = inbound_date_range
        inbound_date_clause = f"AND CAST(COALESCE(dt_im, CASE WHEN UPPER(COALESCE(HVI_NO, '')) LIKE '%MXG%' THEN dt_hvi ELSE NULL END) AS DATE) BETWEEN '{s_date}' AND '{e_date}'"

      if granularity == "Hari":
        date_expr = (
            "STRFTIME(COALESCE(dt_im, CASE WHEN UPPER(COALESCE(HVI_NO, ''))"
            " LIKE '%MXG%' THEN dt_hvi ELSE NULL END), '%d %b')"
        )
      elif granularity == "Week (Minggu)":
        date_expr = (
            "'Week ' || STRFTIME(COALESCE(dt_im, CASE WHEN"
            " UPPER(COALESCE(HVI_NO, '')) LIKE '%MXG%' THEN dt_hvi ELSE NULL"
            " END), '%W')"
        )
      else:
        date_expr = (
            "STRFTIME(COALESCE(dt_im, CASE WHEN UPPER(COALESCE(HVI_NO, ''))"
            " LIKE '%MXG%' THEN dt_hvi ELSE NULL END), '%B')"
        )

      df_inbound_trend = conn.execute(f"""
                SELECT 
                    {date_expr} AS periode,
                    COUNT(DISTINCT AWB) AS total_awb,
                    MIN(CAST(COALESCE(dt_im, CASE WHEN UPPER(COALESCE(HVI_NO, '')) LIKE '%MXG%' THEN dt_hvi ELSE NULL END) AS DATE)) as sort_key
                FROM v_inbound
                {where_sql} AND EXTRACT(YEAR FROM COALESCE(dt_im, CASE WHEN UPPER(COALESCE(HVI_NO, '')) LIKE '%MXG%' THEN dt_hvi ELSE NULL END)) = 2026
                  {inbound_date_clause}
                GROUP BY 1
                ORDER BY sort_key ASC
            """).fetchdf()

      if not df_inbound_trend.empty:
        fig_inbound = go.Figure()
        fig_inbound.add_trace(
            go.Scatter(
                x=df_inbound_trend["periode"],
                y=df_inbound_trend["total_awb"],
                mode="lines+markers",
                fill="tozeroy",
                line=dict(color="#1f77b4", width=3, shape="spline"),
                marker=dict(size=6, color="#1f77b4"),
                fillcolor="rgba(31, 119, 180, 0.3)",
            )
        )
        fig_inbound.update_layout(
            title=dict(
                text="<b>TOTAL INBOUND 2026 (FILTERED)</b>",
                x=0.5,
                xanchor="center",
                font=dict(size=18, color="#FFFFFF"),
            ),
            xaxis=dict(
                showgrid=False,
                zeroline=False,
                tickfont=dict(color="#CCCCCC"),
            ),
            yaxis=dict(
                showgrid=True,
                gridcolor="rgba(255, 255, 255, 0.1)",
                gridwidth=1,
                griddash="dash",
                zeroline=False,
                tickfont=dict(color="#CCCCCC"),
            ),
            plot_bgcolor="rgba(15, 32, 67, 0.8)",
            paper_bgcolor="rgba(0, 0, 0, 0)",
            margin=dict(l=40, r=40, t=60, b=40),
            height=350,
        )
        st.plotly_chart(fig_inbound, use_container_width=True)
      else:
        st.info(
            "Tidak ada data Inbound untuk filter dan rentang tanggal tersebut."
        )

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
            cell_values.append([
                f"{int(val):,}" for val in pivot_with_total[col].tolist()
            ])

        cell_colors = []
        n_rows = len(pivot_with_total)

        for col in headers:
          col_color_list = []
          for row_idx in range(n_rows):
            if row_idx == n_rows - 1:
              col_color_list.append("#222222")
            elif col == "Cabang" or col == "Grand Total":
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
                font=dict(size=16, color="#FFFFFF"),
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

      df_unrun = conn.execute(f"""
                SELECT Cabang, aging_group, COUNT(DISTINCT AWB) AS total_awb 
                FROM v_inbound
                {where_sql} AND Status_Shipment IN ('UnRunsheet', 'UnRunsheet with PRA')
                GROUP BY Cabang, aging_group
            """).fetchdf()
      render_plotly_table_matrix(
          df_unrun,
          "📊 2. Matrix Infografis UnRunsheet Per Cabang (Aging Last_Inbound)",
          "Matrix_UnRunsheet_Cabang.csv",
          "📥 Download Data Matrix UnRunsheet (CSV)",
      )

      st.markdown("---")

      df_succ = conn.execute(f"""
                SELECT Cabang, wus_aging_group AS aging_group, COUNT(DISTINCT AWB) AS total_awb
                FROM v_inbound
                {where_sql} AND Status_Shipment IN ('SUCCESS DELIVERY', 'SUCCESS RETURN SHIPPER')
                  AND (HRS = 'UnHRS' OR HRS = '-' OR HRS IS NULL)
                GROUP BY Cabang, wus_aging_group
            """).fetchdf()
      render_plotly_table_matrix(
          df_succ,
          "📊 3. Matrix Infografis Success UnHRS Per Cabang",
          "Matrix_Success_UnHRS_Cabang.csv",
          "📥 Download Data Matrix Success UnHRS (CSV)",
      )

      st.markdown("---")

      df_undel = conn.execute(f"""
                SELECT Cabang, wus_aging_group AS aging_group, COUNT(DISTINCT AWB) AS total_awb
                FROM v_inbound
                {where_sql} AND Status_Shipment = 'UNDELIVERY'
                  AND (HRS = 'UnHRS' OR HRS = '-' OR HRS IS NULL)
                GROUP BY Cabang, wus_aging_group
            """).fetchdf()
      render_plotly_table_matrix(
          df_undel,
          "📊 4. Matrix Infografis Undel UnHRS Per Cabang",
          "Matrix_Undel_UnHRS_Cabang.csv",
          "📥 Download Data Matrix Undel UnHRS (CSV)",
      )

    else:
      st.warning(
          "🔒 Halaman Summary Exec Terkunci. Masukkan password **MXG125** pada"
          " sidebar di sebelah kiri untuk membuka laporan."
      )

except Exception as main_err:
  st.error("⚠️ Terjadi masalah saat mengolah data dashboard:")
  st.exception(main_err)
