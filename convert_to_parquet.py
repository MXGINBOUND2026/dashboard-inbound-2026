import io
import json
import os
import shutil
import duckdb
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

# =========================================================
# KONFIGURASI
# =========================================================
TARGET_GDRIVE_FOLDER_ID = "1qLpozZT-UX2Wm_6F6FzCIjESUkEgbMaP"

TEMP_CSV_DIR = "temp_csv_files"
OUTPUT_PARQUET = "data_inbound_2026.parquet"
SERVICE_ACCOUNT_FILE = "credentials.json"


def get_drive_service():
  SCOPES = ["https://www.googleapis.com/auth/drive"]

  if not os.path.exists(SERVICE_ACCOUNT_FILE):
    raise FileNotFoundError(
        f"File '{SERVICE_ACCOUNT_FILE}' tidak ditemukan di direktori!"
    )

  with open(SERVICE_ACCOUNT_FILE, "r") as f:
    creds_dict = json.load(f)

  # Penanganan mutlak format private_key agar tidak terkena Invalid JWT Signature
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

      # Abaikan file output parquet jika ada di folder
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


# =========================================================
# 1. PROSES UTAMA
# =========================================================
print("1. Menghubungkan ke Google Drive API...")
service = get_drive_service()

if os.path.exists(TEMP_CSV_DIR):
  shutil.rmtree(TEMP_CSV_DIR)

print("2. Mengunduh seluruh file CSV dari Google Drive...")
download_gdrive_folder(service, TARGET_GDRIVE_FOLDER_ID, TEMP_CSV_DIR)

print("\n3. Mengonversi CSV ke Parquet dengan Optimasi Memori DuckDB...")
con = duckdb.connect()
con.execute("PRAGMA memory_limit='3GB';")

csv_pattern = os.path.join(TEMP_CSV_DIR, "**", "*.csv")

con.execute(f"""
    COPY (
        SELECT * FROM read_csv_auto('{csv_pattern}', ignore_errors=true, union_by_name=true)
    ) TO '{OUTPUT_PARQUET}' (FORMAT PARQUET, COMPRESSION 'SNAPPY');
""")

print(f"✅ Selesai! File Parquet lokal berhasil dibuat: {OUTPUT_PARQUET}")

# Membersihkan folder CSV sementara di lokal
shutil.rmtree(TEMP_CSV_DIR)
print("Folder CSV sementara di lokal telah dibersihkan.")

# =========================================================
# 4. UPLOAD / UPDATE PARQUET KEMBALI KE GOOGLE DRIVE
# =========================================================
print("\n4. Mengunggah file Parquet ke Google Drive...")
media = MediaFileUpload(
    OUTPUT_PARQUET, mimetype="application/octet-stream", resumable=True
)

query = f"'{TARGET_GDRIVE_FOLDER_ID}' in parents and name = '{OUTPUT_PARQUET}' and trashed = false"
results = service.files().list(q=query, fields="files(id, name)").execute()
items = results.get("files", [])

if items:
  file_id = items[0]["id"]
  print("Menghapus file lama sebelum mengunggah versi baru...")
  try:
    service.files().delete(fileId=file_id).execute()
  except Exception as e:
    print(f"Info: {e}")

print("Mengunggah file Parquet baru ke Drive...")
file_metadata = {"name": OUTPUT_PARQUET, "parents": [TARGET_GDRIVE_FOLDER_ID]}

# Upload file baru
uploaded_file = (
    service.files()
    .create(body=file_metadata, media_body=media, fields="id")
    .execute()
)

print("🎉 SUKSES! File Parquet baru berhasil diunggah ke Google Drive!")
