import base64
import io
import json
import os
import shutil
import duckdb
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

# =========================================================
# KONFIGURASI
# =========================================================
TARGET_GDRIVE_FOLDER_ID = "1qLpozZT-UX2Wm_6F6FzCIjESUkEgbMaP"
SUPPORTING_GDRIVE_FOLDER_ID = "1EsUiz1Egqzlbk0gpaohr7lZBcR5rqB2Y"

TEMP_CSV_DIR = "temp_csv_files"
OUTPUT_PARQUET = "data_inbound_2026.parquet"

CODING_FILE_NAME = "Coding.xlsx"
SYSCODE_FILE_NAME = "Syscode.xlsx"
USER_ID_FILE_NAME = "User_Id_MXG.xlsx"


def get_drive_service():
  SCOPES = ["https://www.googleapis.com/auth/drive"]
  b64_secret = os.environ.get("GDRIVE_CREDENTIALS_B64")

  if b64_secret:
    try:
      json_bytes = base64.b64decode(b64_secret)
      creds_dict = json.loads(json_bytes.decode("utf-8"))
    except Exception as e:
      raise ValueError(f"Gagal mendecode Base64 GDRIVE_CREDENTIALS_B64: {e}")
  else:
    raise FileNotFoundError(
        "Environment Variable 'GDRIVE_CREDENTIALS_B64' tidak ditemukan!"
    )

  if "private_key" in creds_dict:
    pk = str(creds_dict["private_key"]).replace("\\n", "\n").strip()
    creds_dict["private_key"] = pk

  creds = service_account.Credentials.from_service_account_info(
      creds_dict, scopes=SCOPES
  )
  return build("drive", "v3", credentials=creds)


def download_gdrive_folder(service, folder_id, local_path):
  if not os.path.exists(local_path):
    os.makedirs(local_path)

  page_token = None
  while True:
    query = f"'{folder_id}' in parents and trashed = false"
    results = (
        service.files()
        .list(
            q=query,
            pageSize=100,
            fields="nextPageToken, files(id, name, mimeType)",
            pageToken=page_token,
        )
        .execute()
    )

    items = results.get("files", [])
    for item in items:
      file_id = item["id"]
      file_name = item["name"]
      mime_type = item["mimeType"]

      if file_name == OUTPUT_PARQUET:
        continue

      local_item_path = os.path.join(local_path, file_name)

      if mime_type == "application/vnd.google-apps.folder":
        download_gdrive_folder(service, file_id, local_item_path)
      else:
        print(f"Mengunduh file: {file_name}")
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
          _, done = downloader.next_chunk()

        with open(local_item_path, "wb") as f:
          f.write(fh.getvalue())

    page_token = results.get("nextPageToken")
    if not page_token:
      break


def download_specific_file(service, folder_id, file_name, local_path):
  try:
    query = (
        f"'{folder_id}' in parents and name = '{file_name}' and trashed = false"
    )
    results = service.files().list(q=query, fields="files(id, name)").execute()
    items = results.get("files", [])
    if not items:
      return False
    file_id = items[0]["id"]
    print(f"Mengunduh file pendukung: {file_name}")
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
      _, done = downloader.next_chunk()
    with open(local_path, "wb") as f:
      f.write(fh.getvalue())
    return True
  except Exception as e:
    print(f"Gagal mengunduh {file_name}: {e}")
    return False


print("1. Menghubungkan ke Google Drive API...")
service = get_drive_service()

if os.path.exists(TEMP_CSV_DIR):
  shutil.rmtree(TEMP_CSV_DIR)

print("2. Mengunduh seluruh file CSV utama dan file pendukung dari Google Drive...")
download_gdrive_folder(service, TARGET_GDRIVE_FOLDER_ID, TEMP_CSV_DIR)

local_coding_path = os.path.join(TEMP_CSV_DIR, CODING_FILE_NAME)
local_syscode_path = os.path.join(TEMP_CSV_DIR, SYSCODE_FILE_NAME)
local_user_id_path = os.path.join(TEMP_CSV_DIR, USER_ID_FILE_NAME)

download_specific_file(service, SUPPORTING_GDRIVE_FOLDER_ID, CODING_FILE_NAME, local_coding_path)
download_specific_file(service, SUPPORTING_GDRIVE_FOLDER_ID, SYSCODE_FILE_NAME, local_syscode_path)
download_specific_file(service, SUPPORTING_GDRIVE_FOLDER_ID, USER_ID_FILE_NAME, local_user_id_path)

print("\n3. Membaca dan menyiapkan DataFrame referensi pendukung...")
# Coding
if os.path.exists(local_coding_path):
  df_coding = pd.read_excel(local_coding_path)
  df_coding.columns = df_coding.columns.str.strip()
  df_coding = df_coding[["CODE", "CATEGORY", "DESCRIPTION"]].drop_duplicates(subset=["CODE"])
  df_coding.rename(columns={"CATEGORY": "POD_Category", "DESCRIPTION": "POD_DESC"}, inplace=True)
else:
  df_coding = pd.DataFrame(columns=["CODE", "POD_Category", "POD_DESC"])

# Syscode
if os.path.exists(local_syscode_path):
  df_syscode = pd.read_excel(local_syscode_path)
  df_syscode.columns = df_syscode.columns.str.strip()
else:
  df_syscode = pd.DataFrame()

# User ID
if os.path.exists(local_user_id_path):
  df_user_id = pd.read_excel(local_user_id_path)
  df_user_id.columns = df_user_id.columns.str.strip()
else:
  df_user_id = pd.DataFrame()

print("\n4. Mengolah & memproses seluruh logika di DuckDB sebelum ke Parquet...")
con = duckdb.connect()
con.execute("PRAGMA memory_limit='4GB';")

con.register("df_coding_ref", df_coding)
con.register("syscode_tbl", df_syscode)
con.register("user_id_tbl", df_user_id)

csv_pattern = os.path.join(TEMP_CSV_DIR, "**", "*.csv")

query_duckdb = f"""
    COPY (
        WITH raw AS (
            SELECT 
                main.AWB,
                main.SERVICE,
                main.DEST,
                main.CODING,
                main.WUS_REMARKS,
                main.WUS_OUTGOING_CODE,
                main.STATUS_POD,
                main.RUNSHEET_NO,
                main.PRA_RUNSHEET_NO,
                main.DATE_RUNSHEET,
                main.AWB_CANCEL,
                main.CONNOTE_RETURN_RT,
                main.CONNOTE_RETURN_RF,
                main.INBOUND_MANIFEST,
                main.INBOUND_MANIFEST_DATE,
                main.USER_IM,
                main.HVO_NO,
                main.HVO_DATE,
                main.HVO_HUB_NAME,
                main.HVI_NO,
                main.HVI_DATE,
                main.HVO_HUB_DESTINATION_NAME,
                main.MANIFEST_INBOUND_SUBAGEN_NO,
                main.MANIFEST_INBOUND_SUBAGEN_DATE,
                main.MTI_USER,
                main.MANIFEST_TRANSIT_SUBAGEN_NO,
                main.MANIFEST_TRANSIT_SUBAGEN_DATE,
                main.MTS_USER,
                main.TRANSIT_MANIFEST,
                main.TRANSIT_MANIFEST_DATE,
                main.TRANSIT_MANIFEST_USER,
                main.RECEIVING_DATE,
                main.OUTBOUND_MANIFEST_DATE,
                main.TGL_RECEIVED,
                main.TGL_TARIK_REPORT,
                ref.POD_Category,
                ref.POD_DESC
            FROM read_csv_auto('{csv_pattern}', ignore_errors=true, union_by_name=true) AS main
            LEFT JOIN df_coding_ref AS ref
                ON CAST(main.CODING AS VARCHAR) = CAST(ref.CODE AS VARCHAR)
        ),
        parsed AS (
            SELECT 
                *,
                COALESCE(TRY_STRPTIME(INBOUND_MANIFEST_DATE, '%m/%d/%Y %H:%M'), TRY_STRPTIME(INBOUND_MANIFEST_DATE, '%d-%b-%Y %H:%M'), TRY_STRPTIME(INBOUND_MANIFEST_DATE, '%Y-%m-%d %H:%M'), TRY_CAST(INBOUND_MANIFEST_DATE AS TIMESTAMP)) AS dt_im,
                COALESCE(TRY_STRPTIME(HVO_DATE, '%m/%d/%Y %H:%M'), TRY_STRPTIME(HVO_DATE, '%d-%b-%Y %H:%M'), TRY_STRPTIME(HVO_DATE, '%Y-%m-%d %H:%M'), TRY_CAST(HVO_DATE AS TIMESTAMP)) AS dt_hvo,
                COALESCE(TRY_STRPTIME(HVI_DATE, '%m/%d/%Y %H:%M'), TRY_STRPTIME(HVI_DATE, '%d-%b-%Y %H:%M'), TRY_STRPTIME(HVI_DATE, '%Y-%m-%d %H:%M'), TRY_CAST(HVI_DATE AS TIMESTAMP)) AS dt_hvi,
                COALESCE(TRY_STRPTIME(MANIFEST_INBOUND_SUBAGEN_DATE, '%m/%d/%Y %H:%M'), TRY_STRPTIME(MANIFEST_INBOUND_SUBAGEN_DATE, '%d-%b-%Y %H:%M'), TRY_STRPTIME(MANIFEST_INBOUND_SUBAGEN_DATE, '%Y-%m-%d %H:%M'), TRY_CAST(MANIFEST_INBOUND_SUBAGEN_DATE AS TIMESTAMP)) AS dt_mti,
                COALESCE(TRY_STRPTIME(MANIFEST_TRANSIT_SUBAGEN_DATE, '%m/%d/%Y %H:%M'), TRY_STRPTIME(MANIFEST_TRANSIT_SUBAGEN_DATE, '%d-%b-%Y %H:%M'), TRY_STRPTIME(MANIFEST_TRANSIT_SUBAGEN_DATE, '%Y-%m-%d %H:%M'), TRY_CAST(MANIFEST_TRANSIT_SUBAGEN_DATE AS TIMESTAMP)) AS dt_mts,
                COALESCE(TRY_STRPTIME(TRANSIT_MANIFEST_DATE, '%m/%d/%Y %H:%M'), TRY_STRPTIME(TRANSIT_MANIFEST_DATE, '%d-%b-%Y %H:%M'), TRY_STRPTIME(TRANSIT_MANIFEST_DATE, '%Y-%m-%d %H:%M'), TRY_CAST(TRANSIT_MANIFEST_DATE AS TIMESTAMP)) AS dt_tm,
                COALESCE(TRY_STRPTIME(DATE_RUNSHEET, '%m/%d/%Y %H:%M'), TRY_STRPTIME(DATE_RUNSHEET, '%d-%b-%Y %H:%M'), TRY_STRPTIME(DATE_RUNSHEET, '%Y-%m-%d %H:%M'), TRY_CAST(DATE_RUNSHEET AS TIMESTAMP)) AS dt_run,
                COALESCE(TRY_STRPTIME(TGL_RECEIVED, '%m/%d/%Y %H:%M'), TRY_STRPTIME(TGL_RECEIVED, '%d-%b-%Y %H:%M'), TRY_STRPTIME(TGL_RECEIVED, '%Y-%m-%d %H:%M'), TRY_CAST(TGL_RECEIVED AS TIMESTAMP)) AS dt_tgl_rec
            FROM raw
        ),
        joined_loc AS (
            SELECT 
                p.*,
                u_im.USER_ZONE_NAME AS IM_LOCATION,
                u_mts.USER_ZONE_NAME AS MTS_LOCATION,
                u_mti.USER_ZONE_NAME AS MTI_LOCATION,
                CASE WHEN UPPER(COALESCE(p.TRANSIT_MANIFEST, '')) LIKE '%MXG%' THEN u_tm.USER_ZONE_NAME ELSE NULL END AS TM_LOCATION
            FROM parsed p
            LEFT JOIN user_id_tbl u_im ON p.USER_IM = u_im.USER_ID
            LEFT JOIN user_id_tbl u_mts ON p.MTS_USER = u_mts.USER_ID
            LEFT JOIN user_id_tbl u_mti ON p.MTI_USER = u_mti.USER_ID
            LEFT JOIN user_id_tbl u_tm ON p.TRANSIT_MANIFEST_USER = u_tm.USER_ID
        ),
        calc_inbound AS (
            SELECT 
                j.*,
                GREATEST(
                    CASE WHEN UPPER(COALESCE(INBOUND_MANIFEST, '')) LIKE '%MXG%' THEN dt_im ELSE NULL END,
                    CASE WHEN UPPER(COALESCE(HVO_NO, '')) LIKE '%MXG%' THEN dt_hvo ELSE NULL END,
                    CASE WHEN UPPER(COALESCE(HVI_NO, '')) LIKE '%MXG%' THEN dt_hvi ELSE NULL END,
                    CASE WHEN UPPER(COALESCE(MANIFEST_INBOUND_SUBAGEN_NO, '')) LIKE '%MXG%' THEN dt_mti ELSE NULL END,
                    CASE WHEN UPPER(COALESCE(MANIFEST_TRANSIT_SUBAGEN_NO, '')) LIKE '%MXG%' THEN dt_mts ELSE NULL END,
                    CASE WHEN UPPER(COALESCE(TRANSIT_MANIFEST, '')) LIKE '%MXG%' THEN dt_tm ELSE NULL END
                ) AS valid_inbound_date
            FROM joined_loc j
        ),
        processed AS (
            SELECT 
                c.*,
                CASE 
                    WHEN valid_inbound_date IS NULL THEN NULL
                    WHEN valid_inbound_date = dt_im  THEN IM_LOCATION
                    WHEN valid_inbound_date = dt_hvo THEN TRY_CAST(c.HVO_HUB_NAME AS VARCHAR)
                    WHEN valid_inbound_date = dt_hvi THEN TRY_CAST(c.HVO_HUB_DESTINATION_NAME AS VARCHAR)
                    WHEN valid_inbound_date = dt_mti THEN MTI_LOCATION
                    WHEN valid_inbound_date = dt_mts THEN MTS_LOCATION
                    WHEN valid_inbound_date = dt_tm  THEN TM_LOCATION
                    ELSE NULL
                END AS LAST_LOCATION,
                
                CASE 
                    WHEN AWB_CANCEL IS NOT NULL AND TRIM(CAST(AWB_CANCEL AS VARCHAR)) != '' AND UPPER(TRIM(CAST(AWB_CANCEL AS VARCHAR))) != 'N' THEN 'AWB Cancel'
                    WHEN RECEIVING_DATE IS NULL OR TRIM(CAST(RECEIVING_DATE AS VARCHAR)) = '' OR RECEIVING_DATE = '-' THEN 'UnReceiving'
                    WHEN (OUTBOUND_MANIFEST_DATE IS NULL OR TRIM(CAST(OUTBOUND_MANIFEST_DATE AS VARCHAR)) = '' OR OUTBOUND_MANIFEST_DATE = '-')
                         AND NOT (UPPER(SERVICE) LIKE '%CTC%' AND HVO_DATE IS NULL AND UPPER(COALESCE(HVO_NO, '')) LIKE '%MXG%' AND OUTBOUND_MANIFEST_DATE IS NOT NULL) 
                    THEN 'UnOutbound'
                    WHEN (INBOUND_MANIFEST_DATE IS NULL OR TRIM(CAST(INBOUND_MANIFEST_DATE AS VARCHAR)) = '' OR INBOUND_MANIFEST_DATE = '-')
                         AND NOT (UPPER(SERVICE) LIKE '%CTC%' AND HVI_DATE IS NULL AND UPPER(COALESCE(HVI_NO, '')) LIKE '%MXG%' AND INBOUND_MANIFEST_DATE IS NOT NULL) 
                    THEN 'UnInbound'
                    WHEN UPPER(COALESCE(MANIFEST_TRANSIT_SUBAGEN_NO, '')) LIKE '%MXG%' 
                         AND (MANIFEST_INBOUND_SUBAGEN_NO IS NULL OR TRIM(CAST(MANIFEST_INBOUND_SUBAGEN_NO AS VARCHAR)) = '' OR MANIFEST_INBOUND_SUBAGEN_NO = '-') 
                    THEN 'UnMTI'
                    WHEN (DATE_RUNSHEET IS NULL OR TRIM(CAST(DATE_RUNSHEET AS VARCHAR)) = '' OR DATE_RUNSHEET = '-')
                         AND UPPER(COALESCE(PRA_RUNSHEET_NO, '')) LIKE '%MXG%' 
                    THEN 'UnRunsheet with PRA'
                    WHEN DATE_RUNSHEET IS NULL OR TRIM(CAST(DATE_RUNSHEET AS VARCHAR)) = '' OR DATE_RUNSHEET = '-' 
                    THEN 'UnRunsheet'
                    WHEN (DATE_RUNSHEET IS NULL OR TRIM(CAST(DATE_RUNSHEET AS VARCHAR)) = '' OR DATE_RUNSHEET = '-')
                         AND UPPER(COALESCE(WUS_OUTGOING_CODE, '')) LIKE '%HOC%'
                    THEN 'UnRunsheet AU (HOC)'
                    WHEN DATE_RUNSHEET IS NOT NULL AND CAST(dt_run AS DATE) = CURRENT_DATE 
                         AND (CODING IS NULL OR TRIM(CAST(CODING AS VARCHAR)) = '' OR CODING = '-') 
                    THEN 'On Delivery Today'
                    WHEN DATE_RUNSHEET IS NOT NULL 
                         AND (CODING IS NULL OR TRIM(CAST(CODING AS VARCHAR)) = '' OR CODING = '-') 
                    THEN 'Open POD'
                    WHEN UPPER(COALESCE(STATUS_POD, '')) IN ('SUCCESS', 'RETURN SHIPPER') 
                         AND UPPER(COALESCE(WUS_OUTGOING_CODE, '')) NOT LIKE '%HRS%' 
                    THEN 'Success UnHRS'
                    WHEN (CODING IS NOT NULL AND TRIM(CAST(CODING AS VARCHAR)) != '' AND CODING != '-')
                         AND (WUS_REMARKS IS NULL OR TRIM(CAST(WUS_REMARKS AS VARCHAR)) = '' OR WUS_REMARKS = '-')
                         AND UPPER(COALESCE(POD_Category, '')) NOT IN (
                             'CLAIM MISSING PROCESS', 'CLAIM DAMAGE PROCESS', 'CLOSE BY SYSTEM (ORIGIN)', 
                             'CLOSE BY SYSTEM (DEST)', 'CUSTOMER REQUEST CANCEL', 'CUSTOMER REQUEST', 
                             'SUCCESS CLAIM', 'BREACH', 'KIRIMAN DISITA OLEH BEA CUKAI', 
                             'KIRIMAN DISITA OLEH KEPOLISIAN', 'KIRIMAN RETURN BREACH', 'MISSING'
                         ) 
                    THEN 'UnHRS'
                    ELSE COALESCE(POD_Category, 'UNKNOWN')
                END AS Status_Shipment,

                CASE 
                    WHEN dt_tgl_rec IS NOT NULL THEN DATEDIFF('day', dt_tgl_rec, CURRENT_TIMESTAMP)
                    ELSE DATEDIFF('day', valid_inbound_date, CURRENT_TIMESTAMP)
                END AS age_days
            FROM calc_inbound c
        )
        SELECT 
            COALESCE(w.AWB, '-') AS AWB,
            COALESCE(w.SERVICE, '-') AS SERVICE,
            COALESCE(w.WUS_REMARKS, '-') AS WUS_REMARKS,
            w.valid_inbound_date AS Last_Inbound,
            COALESCE(w.LAST_LOCATION, 'UNKNOWN LOCATION') AS LAST_LOCATION,
            COALESCE(s.KABUPATEN, 'UNKNOWN CABANG') AS Cabang,
            COALESCE(s."ZONA DELEVERY (QCE)", 'UNKNOWN ZONA') AS Zona,
            COALESCE(s.KECAMATAN, 'UNKNOWN KECAMATAN') AS Kecamatan,
            w.Status_Shipment AS Status_Shipment,
            w.age_days AS age_days,
            COALESCE(w.POD_Category, '-') AS POD_Category,
            COALESCE(w.POD_DESC, '-') AS POD_DESC,
            CASE 
                WHEN age_days <= 0 THEN 'H+0'
                WHEN age_days = 1 THEN 'H+1'
                WHEN age_days = 2 THEN 'H+2'
                WHEN age_days = 3 THEN 'H+3'
                WHEN age_days = 4 THEN 'H+4'
                WHEN age_days = 5 THEN 'H+5'
                WHEN age_days = 6 THEN 'H+6'
                WHEN age_days = 7 THEN 'H+7'
                ELSE '>H+7'
            END AS aging_group,
            w.TGL_TARIK_REPORT
        FROM processed w
        LEFT JOIN syscode_tbl s ON w.DEST = s.SYSCODE
    ) TO '{OUTPUT_PARQUET}' (FORMAT PARQUET, COMPRESSION 'SNAPPY');
"""

con.execute(query_duckdb)
print(f"✅ Selesai! File Parquet siap saji berhasil dibuat: {OUTPUT_PARQUET}")
shutil.rmtree(TEMP_CSV_DIR)

print("\n5. Memperbarui file Parquet di Google Drive...")
media = MediaFileUpload(
    OUTPUT_PARQUET, mimetype="application/octet-stream", resumable=True
)
query = f"'{TARGET_GDRIVE_FOLDER_ID}' in parents and name = '{OUTPUT_PARQUET}' and trashed = false"
results = service.files().list(q=query, fields="files(id, name)").execute()
items = results.get("files", [])

if items:
  file_id = items[0]["id"]
  service.files().update(fileId=file_id, media_body=media).execute()
  print("🎉 SUKSES! File Parquet di-update di Google Drive!")
else:
  file_metadata = {"name": OUTPUT_PARQUET, "parents": [TARGET_GDRIVE_FOLDER_ID]}
  service.files().create(body=file_metadata, media_body=media, fields="id").execute()
  print("🎉 SUKSES! File Parquet baru berhasil diunggah ke Google Drive!")
