import os
import io
import shutil
import duckdb
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

# =========================================================
# KONFIGURASI
# =========================================================
# Ganti dengan FOLDER_ID Google Drive Anda (bagian terakhir dari URL folder GDrive)
# Contoh URL: https://drive.google.com/drive/folders/1qLpozZT-UX2Wm_6F6FzCIjESUkEgbMaP
TARGET_GDRIVE_FOLDER_ID = "1qLpozZT-UX2Wm_6F6FzCIjESUkEgbMaP"

TEMP_CSV_DIR = "temp_csv_files"
OUTPUT_PARQUET = "data_inbound_2026.parquet"
SERVICE_ACCOUNT_FILE = "credentials.json"

def get_drive_service():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=['https://www.googleapis.com/auth/drive']
    )
    return build('drive', 'v3', credentials=creds)

def download_gdrive_folder(service, folder_id, local_path):
    if not os.path.exists(local_path):
        os.makedirs(local_path)
    
    page_token = None
    while True:
        query = f"'{folder_id}' in parents and trashed = false"
        results = service.files().list(
            q=query,
            pageSize=100,
            fields="nextPageToken, files(id, name, mimeType)",
            pageToken=page_token
        ).execute()
        
        items = results.get('files', [])
        for item in items:
            file_id = item['id']
            file_name = item['name']
            mime_type = item['mimeType']
            local_item_path = os.path.join(local_path, file_name)
            
            if mime_type == 'application/vnd.google-apps.folder':
                # Jika berupa subfolder, lakukan rekursif ke dalam
                download_gdrive_folder(service, file_id, local_item_path)
            else:
                # Download file CSV
                print(f"Mengunduh file: {file_name}")
                request = service.files().get_media(fileId=file_id)
                fh = io.BytesIO()
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while not done:
                    status, done = downloader.next_chunk()
                
                with open(local_item_path, 'wb') as f:
                    f.write(fh.getvalue())
                    
        page_token = results.get('nextPageToken')
        if not page_token:
            break

# =========================================================
# 1. PROSES UTAMA
# =========================================================
print("1. Menghubungkan ke Google Drive API...")
service = get_drive_service()

if os.path.exists(TEMP_CSV_DIR):
    shutil.rmtree(TEMP_CSV_DIR)

print("2. Mengunduh seluruh file CSV dari folder Google Drive...")
download_gdrive_folder(service, TARGET_GDRIVE_FOLDER_ID, TEMP_CSV_DIR)

print("\n3. Mengonversi seluruh file CSV ke format Parquet via DuckDB...")
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
# 4. UPLOAD / UPDATE PARQUET KEMBALI KE GOOGLE DRIVE
# =========================================================
print("\n4. Mengunggah file Parquet ke Google Drive...")
media = MediaFileUpload(OUTPUT_PARQUET, resumable=True)

# Cek apakah file parquet sudah ada di folder tujuan
query = f"'{TARGET_GDRIVE_FOLDER_ID}' in parents and name = '{OUTPUT_PARQUET}' and trashed = false"
results = service.files().list(q=query, fields="files(id, name)").execute()
items = results.get('files', [])

if items:
    file_id = items[0]['id']
    print("Memperbarui file Parquet yang sudah ada di Drive...")
    service.files().update(fileId=file_id, media_body=media).execute()
    print("🎉 SUKSES! File Parquet berhasil diperbarui di Google Drive!")
else:
    print("Mengunggah file Parquet baru ke Drive...")
    file_metadata = {
        'name': OUTPUT_PARQUET,
        'parents': [TARGET_GDRIVE_FOLDER_ID]
    }
    service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    print("🎉 SUKSES! File Parquet baru berhasil diunggah ke Google Drive!")
