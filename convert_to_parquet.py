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
SUPPORTING_GDRIVE_FOLDER_ID = (
    "1EsUiz1Egqzlbk0gpaohr7lZBcR5rqB2Y"  # Folder DATA FILTER
)

TEMP_CSV_DIR = "temp_csv_files"
OUTPUT_PARQUET = "data_inbound_2026.parquet"
CODING_FILE_NAME = "Coding.xlsx"


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
    pk = str(creds_dict["private_key"])
    pk = pk.replace("\\n", "\n")
    creds_dict["private_key"] = pk.strip()

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
          status, done = downloader.next_chunk()

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
      status, done = downloader.next_chunk()
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

print("2. Mengunduh seluruh file CSV utama dari Google Drive...")
download_gdrive_folder(service, TARGET_GDRIVE_FOLDER_ID, TEMP_CSV_DIR)

local_coding_path = os.path.join(TEMP_CSV_DIR, CODING_FILE_NAME)
download_specific_file(
    service, SUPPORTING_GDRIVE_FOLDER_ID, CODING_FILE_NAME, local_coding_path
)

print("\n3. Membaca file pendukung Coding.xlsx...")
if os.path.exists(local_coding_path):
  df_coding = pd.read_excel(local_coding_path)
  df_coding.columns = df_coding.columns.str.strip()
  df_coding = df_coding[["CODE", "CATEGORY", "DESCRIPTION"]].drop_duplicates(
      subset=["CODE"]
  )
  df_coding.rename(
      columns={"CATEGORY": "POD_Category", "DESCRIPTION": "POD_DESC"},
      inplace=True,
  )
else:
  df_coding = pd.DataFrame(columns=["CODE", "POD_Category", "POD_DESC"])

print(
    "\n4. Mengonversi CSV, melakukan LEFT JOIN Coding, dan memproses kolom"
    " logika via DuckDB..."
)
con = duckdb.connect()
con.execute("PRAGMA memory_limit='3GB';")
con.register("df_coding_ref", df_coding)
csv_pattern = os.path.join(TEMP_CSV_DIR, "**", "*.csv")

query_duckdb = f"""
    COPY (
        WITH raw_joined AS (
            SELECT 
                main.*,
                ref.POD_Category,
                ref.POD_DESC
            FROM read_csv_auto('{csv_pattern}', ignore_errors=true, union_by_name=true) AS main
            LEFT JOIN df_coding_ref AS ref
                ON CAST(main.CODING AS VARCHAR) = CAST(ref.CODE AS VARCHAR)
        )
        SELECT * FROM raw_joined
    ) TO '{OUTPUT_PARQUET}' (FORMAT PARQUET, COMPRESSION 'SNAPPY');
"""

con.execute(query_duckdb)
print(f"✅ Selesai! File Parquet berhasil diperbarui: {OUTPUT_PARQUET}")
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
  print("🎉 SUKSES! File Parquet berhasil di-update di Google Drive!")
else:
  file_metadata = {"name": OUTPUT_PARQUET, "parents": [TARGET_GDRIVE_FOLDER_ID]}
  service.files().create(body=file_metadata, media_body=media, fields="id").execute()
  print("🎉 SUKSES! File Parquet baru berhasil diunggah ke Google Drive!")
