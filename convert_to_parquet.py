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

TEMP_CSV_DIR = "temp_csv_files"
OUTPUT_PARQUET = "data_inbound_2026.parquet"


def get_drive_service():
  SCOPES = ["https://www.googleapis.com/auth/drive"]

  # Membaca string Base64 langsung dari GitHub Actions Environment Variable
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
# 1. PROSES UTAMA & MENGUNDUH DATA DARI GOOGLE DRIVE
# =========================================================
print("1. Menghubungkan ke Google Drive API...")
service = get_drive_service()

if os.path.exists(TEMP_CSV_DIR):
  shutil.rmtree(TEMP_CSV_DIR)

print("2. Mengunduh seluruh file (CSV & Folder Filter) dari Google Drive...")
download_gdrive_folder(service, TARGET_GDRIVE_FOLDER_ID, TEMP_CSV_DIR)

# =========================================================
# 2. MEMBACA FILE PENDUKUNG CODING.XLSX
# =========================================================
print("\n3. Membaca file pendukung Coding.xlsx...")
coding_file_path = None

# Mencari file Coding.xlsx secara rekursif di dalam folder hasil unduhan
for root, dirs, files in os.walk(TEMP_CSV_DIR):
  for f in files:
    if f.lower() == "coding.xlsx":
      coding_file_path = os.path.join(root, f)
      break

if coding_file_path and os.path.exists(coding_file_path):
  print(f"File Coding.xlsx ditemukan di: {coding_file_path}")
  df_coding = pd.read_excel(coding_file_path)

  # Bersihkan spasi pada nama kolom dan ambil kolom referensi
  df_coding.columns = df_coding.columns.str.strip()
  df_coding = df_coding[["CODE", "CATEGORY", "DESCRIPTION"]].drop_duplicates(
      subset=["CODE"]
  )
  # Ubah nama kolom sesuai permintaan
  df_coding.rename(
      columns={"CATEGORY": "Categori POD", "DESCRIPTION": "Desc POD"},
      inplace=True,
  )
else:
  print("⚠️ Warning: File Coding.xlsx tidak ditemukan. Membuat referensi kosong.")
  df_coding = pd.DataFrame(columns=["CODE", "Categori POD", "Desc POD"])

# =========================================================
# 3. MENGONVERSI CSV & JOIN DENGAN DUCKDB
# =========================================================
print(
    "\n4. Mengonversi CSV dan melakukan LEFT JOIN dengan referensi POD via"
    " DuckDB..."
)
con = duckdb.connect()
con.execute("PRAGMA memory_limit='3GB';")

# Daftarkan DataFrame referensi Coding ke dalam DuckDB
con.register("df_coding_ref", df_coding)

csv_pattern = os.path.join(TEMP_CSV_DIR, "**", "*.csv")

query_duckdb = f"""
    COPY (
        SELECT 
            main.*,
            ref."Categori POD",
            ref."Desc POD"
        FROM read_csv_auto('{csv_pattern}', ignore_errors=true, union_by_name=true) AS main
        LEFT JOIN df_coding_ref AS ref
            ON CAST(main.CODING AS VARCHAR) = CAST(ref.CODE AS VARCHAR)
    ) TO '{OUTPUT_PARQUET}' (FORMAT PARQUET, COMPRESSION 'SNAPPY');
"""

con.execute(query_duckdb)
print(
    f"✅ Selesai! File Parquet dengan kolom tambahan 'Categori POD' dan"
    f" 'Desc POD' berhasil dibuat: {OUTPUT_PARQUET}"
)

# Membersihkan folder sementara di lokal
shutil.rmtree(TEMP_CSV_DIR)
print("Folder sementara di lokal telah dibersihkan.")

# =========================================================
# 4. UPLOAD / UPDATE PARQUET KEMBALI KE GOOGLE DRIVE
# =========================================================
print(
    "\n5. Memperbarui (Update) file Parquet yang sudah ada di Google Drive..."
)
media = MediaFileUpload(
    OUTPUT_PARQUET, mimetype="application/octet-stream", resumable=True
)

query = f"'{TARGET_GDRIVE_FOLDER_ID}' in parents and name = '{OUTPUT_PARQUET}' and trashed = false"
results = service.files().list(q=query, fields="files(id, name)").execute()
items = results.get("files", [])

if items:
  # Jika file sudah ada, timpa (update) isinya tanpa mengubah ID
  file_id = items[0]["id"]
  print(f"Menimpa isi file lama (ID: {file_id})...")
  updated_file = (
      service.files().update(fileId=file_id, media_body=media).execute()
  )
  print("🎉 SUKSES! File Parquet berhasil diperbarui di Google Drive!")
else:
  # Jika file belum ada sama sekali, buat baru
  print("File Parquet belum ada, membuat file baru...")
  file_metadata = {"name": OUTPUT_PARQUET, "parents": [TARGET_GDRIVE_FOLDER_ID]}
  uploaded_file = (
      service.files()
      .create(body=file_metadata, media_body=media, fields="id")
      .execute()
  )
  print("🎉 SUKSES! File Parquet baru berhasil diunggah ke Google Drive!")
