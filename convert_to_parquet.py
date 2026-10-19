import gdown
import duckdb
import os
import shutil
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaFileUpload

# =========================================================
# KONFIGURASI FOLDER & FILE
# =========================================================
# URL folder GDrive asal (tempat CSV berada)
GDRIVE_FOLDER_URL = "https://drive.google.com/drive/folders/1qLpozZT-UX2Wm_6F6FzCIjESUkEgbMaP?usp=drive_link"

# ID Folder GDrive tujuan tempat menyimpan file Parquet hasil konversi
TARGET_GDRIVE_FOLDER_ID = "1qLpozZT-UX2Wm_6F6FzCIjESUkEgbMaP"

TEMP_CSV_DIR = "temp_csv_files"
OUTPUT_PARQUET = "data_inbound_2026.parquet"
SERVICE_ACCOUNT_FILE = "credentials.json"

# =========================================================
# 1. UNDUH CSV DARI GOOGLE DRIVE
# =========================================================
print("1. Mengunduh folder CSV dari Google Drive...")
if os.path.exists(TEMP_CSV_DIR):
    shutil.rmtree(TEMP_CSV_DIR)

gdown.download_folder(GDRIVE_FOLDER_URL, output=TEMP_CSV_DIR, quiet=False)

# =========================================================
# 2. KONVERSI CSV KE PARQUET (DUCKDB)
# =========================================================
print("\n2. Mengonversi seluruh file CSV ke format Parquet via DuckDB...")
con = duckdb.connect()
csv_pattern = os.path.join(TEMP_CSV_DIR, "**", "*.csv")

con.execute(f"""
    COPY (
        SELECT * FROM read_csv_auto('{csv_pattern}', ignore_errors=true)
    ) TO '{OUTPUT_PARQUET}' (FORMAT PARQUET, COMPRESSION 'SNAPPY');
""")

print(f"✅ Selesai! File Parquet lokal berhasil dibuat: {OUTPUT_PARQUET}")

# Membersihkan folder CSV sementara di lokal
shutil.rmtree(TEMP_CSV_DIR)
print("Folder CSV sementara di lokal telah dibersihkan.")

# =========================================================
# 3. OTOMATISASI UPLOAD KE GOOGLE DRIVE
# =========================================================
def upload_or_update_parquet(filename, folder_id):
    print("\n3. Mengunggah file Parquet secara otomatis ke Google Drive...")
    
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        print(f"❌ Error: File '{SERVICE_ACCOUNT_FILE}' tidak ditemukan di folder proyek!")
        return

    SCOPES = ['https://www.googleapis.com/auth/drive.file']
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    
    service = build('drive', 'v3', credentials=creds)

    # Cek apakah file parquet dengan nama yang sama sudah ada di folder Drive
    query = f"'{folder_id}' in parents and name = '{filename}' and trashed = false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    items = results.get('files', [])

    media = MediaFileUpload(filename, resumable=True)

    if items:
        # Jika file sudah ada, lakukan update (overwrite) agar File ID tidak berubah
        file_id = items[0]['id']
        print(f"File lama ditemukan di Drive. Memperbarui isi file...")
        updated_file = service.files().update(
            fileId=file_id,
            media_body=media
        ).execute()
        print(f"🎉 SUKSES! File Parquet berhasil diperbarui di Google Drive!")
    else:
        # Jika file belum ada, upload sebagai file baru
        print("Mengunggah sebagai file baru...")
        file_metadata = {
            'name': filename,
            'parents': [folder_id]
        }
        new_file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()
        print(f"🎉 SUKSES! File Parquet baru berhasil diunggah ke Google Drive!")

# Jalankan proses upload otomatis
upload_or_update_parquet(OUTPUT_PARQUET, TARGET_GDRIVE_FOLDER_ID)
