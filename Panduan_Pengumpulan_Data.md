# Panduan Pengumpulan Data – Trending YouTube Indonesia

**Proyek:** Analisis Data Trending YouTube Indonesia untuk Mengidentifikasi Faktor Penentu Popularitas Video Menggunakan Apache Spark
**Mata kuliah:** Big Data (7ITA1)
**Sumber:** `Kanban_Trending_YouTube.pdf`

---

## 1. Ringkasan isi PDF

PDF berisi Kanban Board dengan 20 sticky note yang dibagi per fase (warna):

| Warna | Fase | Kartu |
|---|---|---|
| Kuning | Perencanaan | #01–#04 |
| Biru | Pengumpulan Data | #05–#09 |
| Hijau | Preprocessing & EDA | #10–#12 |
| Pink | Modeling & Evaluasi | #13–#15 |
| Oranye | Visualisasi, Laporan & Presentasi | #16–#20 |

Posisi awal kartu:

- **Done:** #01 Bentuk kelompok, #02 Pilih topik, #03 Tentukan judul
- **Doing:** #04 Rumusan masalah, #05 Buat API key
- **To Do:** #06 Script ambil data trending, #07 Jadwal ambil data harian, #08 Cari dataset pendukung
- **Backlog:** #09–#20

Daftar lengkap kartu:

| No | Judul | Keterangan | Bukti selesai | Kolom awal |
|---|---|---|---|---|
| 01 | Bentuk kelompok | Tentukan ketua dan bagi peran anggota | Daftar nama & peran | Done |
| 02 | Pilih topik | Sepakati topik trending YouTube Indonesia | Topik tercatat | Done |
| 03 | Tentukan judul | Rumuskan judul proyek | Judul final | Done |
| 04 | Rumusan masalah | 3–5 pertanyaan: kategori terpopuler, jam upload terbaik, pengaruh durasi/tag, prediksi populer | Dokumen rumusan masalah | Doing |
| 05 | Buat API key | Aktifkan YouTube Data API v3 di Google Cloud Console | API key berfungsi | Doing |
| 06 | Script ambil data trending | Python mengambil video trending (chart=mostPopular, regionCode=ID) | File CSV/JSON pertama | To Do |
| 07 | Jadwal ambil data harian | Jalankan script otomatis tiap hari 3–4 minggu | Kumpulan file harian | To Do |
| 08 | Cari dataset pendukung | Dataset trending YouTube publik sebagai pembanding/tambahan | Dataset terunduh | To Do |
| 09 | Simpan data ke storage | Gabungkan seluruh data ke HDFS / Google Drive | Satu dataset terpusat | Backlog |
| 10 | Cleaning data | Hapus duplikat, ubah format durasi (ISO 8601), tanggal, tag | Dataset bersih | Backlog |
| 11 | Feature engineering | Jam & hari upload, rasio like/view, panjang judul, jumlah tag, lama trending | Tabel fitur | Backlog |
| 12 | EDA dengan PySpark | Statistik deskriptif dan grafik awal | Notebook EDA + grafik | Backlog |
| 13 | Labeling popularitas | Kriteria "populer" (mis. views di atas persentil 75) | Kolom label | Backlog |
| 14 | Training model | Random Forest / Logistic Regression di Spark MLlib | Model tersimpan | Backlog |
| 15 | Evaluasi model | Akurasi, precision, recall, F1-score, feature importance | Tabel hasil evaluasi | Backlog |
| 16 | Dashboard visualisasi | Looker Studio atau Streamlit | Link dashboard | Backlog |
| 17 | Interpretasi hasil | Simpulkan faktor penentu popularitas video | Poin kesimpulan | Backlog |
| 18 | Laporan akhir | Bab 1–5 + lampiran kode | File laporan | Backlog |
| 19 | Slide presentasi | 10–12 slide | File slide | Backlog |
| 20 | Latihan & demo | Gladi presentasi dan demo dashboard | Siap presentasi | Backlog |

---

## 2. Cara pengumpulan data (kartu biru #05–#09)

### #05 – Buat API key

1. Buka <https://console.cloud.google.com>, lalu buat project baru.
2. Masuk ke **APIs & Services → Library**, cari **YouTube Data API v3**, lalu klik **Enable**.
3. Masuk ke **APIs & Services → Credentials → Create Credentials → API key**, lalu salin key-nya.
4. Simpan key di file `api_key.txt` (satu baris, isinya key saja) di folder yang sama dengan script, **atau** set sebagai environment variable `YOUTUBE_API_KEY`.
5. Jangan upload `api_key.txt` ke GitHub atau grup chat.

**Bukti selesai:** script di #06 berhasil jalan tanpa error.

### #06 – Script ambil data trending

- Script memanggil endpoint `videos` dengan `chart=mostPopular` dan `regionCode=ID`. Hasilnya biasanya sekitar 200 video per hari.
- Hasil disimpan ke `data/trending_YYYY-MM-DD.csv`. Tanggalnya memakai zona waktu WIB.
- Hanya memakai library bawaan Python 3, jadi tidak perlu `pip install`.
- Kuota yang terpakai sekitar 5 unit per hari, dari jatah gratis 10.000 unit per hari.

Kolom yang disimpan:

| Kolom | Isi |
|---|---|
| `trending_date`, `fetched_at` | Tanggal dan waktu data diambil (WIB) |
| `rank` | Urutan di daftar trending |
| `video_id`, `title` | ID dan judul video |
| `channel_id`, `channel_title` | Channel pengunggah |
| `category_id`, `category_name` | Kategori (nama dalam bahasa Indonesia) |
| `publish_time` | Waktu upload (UTC, format ISO) |
| `tags` | Tag, dipisah dengan `\|` |
| `duration` | Durasi format ISO 8601, mis. `PT12M5S` |
| `definition`, `caption` | Kualitas video (hd/sd) dan ada subtitle atau tidak |
| `view_count`, `like_count`, `comment_count` | Statistik saat diambil |
| `comments_disabled` | `True` jika komentar dimatikan |
| `thumbnail_link`, `description` | Thumbnail dan deskripsi |

Cara menjalankan:

```bash
python ambil_trending.py
```

**Bukti selesai:** file CSV pertama di folder `data/`.

Isi lengkap `ambil_trending.py`:

```python
"""
Ambil daftar video trending YouTube Indonesia (chart=mostPopular, regionCode=ID)
lalu simpan ke CSV harian: data/trending_YYYY-MM-DD.csv

Cara pakai:
    1. Set API key sebagai environment variable YOUTUBE_API_KEY
       (atau tulis di file api_key.txt di folder yang sama).
    2. python ambil_trending.py

Hanya memakai library bawaan Python (tidak perlu pip install).
Kuota: ±5 unit per hari (batas gratis 10.000 unit/hari).
"""

import csv
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

REGION = "ID"
BASE_URL = "https://www.googleapis.com/youtube/v3"
FOLDER = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(FOLDER, "data")
WIB = timezone(timedelta(hours=7))

KOLOM = [
    "trending_date", "fetched_at", "rank", "video_id", "title",
    "channel_id", "channel_title", "category_id", "category_name",
    "publish_time", "tags", "duration", "definition", "caption",
    "view_count", "like_count", "comment_count",
    "comments_disabled", "thumbnail_link", "description",
]


def baca_api_key():
    key = os.environ.get("YOUTUBE_API_KEY")
    path = os.path.join(FOLDER, "api_key.txt")
    if not key and os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            key = f.read().strip()
    if not key:
        sys.exit("API key tidak ditemukan. Set YOUTUBE_API_KEY atau buat api_key.txt")
    return key


def panggil_api(endpoint, params):
    url = f"{BASE_URL}/{endpoint}?{urllib.parse.urlencode(params)}"
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            return json.load(resp)
    except urllib.error.HTTPError as e:
        sys.exit(f"Gagal memanggil API ({e.code}): {e.read().decode('utf-8', 'replace')}")


def ambil_kategori(key):
    data = panggil_api("videoCategories", {
        "part": "snippet", "regionCode": REGION, "hl": "id", "key": key,
    })
    return {item["id"]: item["snippet"]["title"] for item in data.get("items", [])}


def ambil_trending(key):
    videos, page_token = [], None
    while True:
        params = {
            "part": "snippet,contentDetails,statistics",
            "chart": "mostPopular",
            "regionCode": REGION,
            "maxResults": 50,
            "key": key,
        }
        if page_token:
            params["pageToken"] = page_token
        data = panggil_api("videos", params)
        videos.extend(data.get("items", []))
        page_token = data.get("nextPageToken")
        if not page_token:
            return videos


def ke_baris(rank, item, kategori, trending_date, fetched_at):
    sn = item.get("snippet", {})
    cd = item.get("contentDetails", {})
    st = item.get("statistics", {})
    thumbs = sn.get("thumbnails", {})
    thumb = (thumbs.get("high") or thumbs.get("default") or {}).get("url", "")
    return {
        "trending_date": trending_date,
        "fetched_at": fetched_at,
        "rank": rank,
        "video_id": item["id"],
        "title": sn.get("title", ""),
        "channel_id": sn.get("channelId", ""),
        "channel_title": sn.get("channelTitle", ""),
        "category_id": sn.get("categoryId", ""),
        "category_name": kategori.get(sn.get("categoryId", ""), ""),
        "publish_time": sn.get("publishedAt", ""),
        "tags": "|".join(sn.get("tags", [])),
        "duration": cd.get("duration", ""),
        "definition": cd.get("definition", ""),
        "caption": cd.get("caption", ""),
        "view_count": st.get("viewCount", ""),
        "like_count": st.get("likeCount", ""),
        "comment_count": st.get("commentCount", ""),
        "comments_disabled": "commentCount" not in st,
        "thumbnail_link": thumb,
        "description": sn.get("description", ""),
    }


def main():
    key = baca_api_key()
    now = datetime.now(WIB)
    trending_date = now.strftime("%Y-%m-%d")
    fetched_at = now.isoformat(timespec="seconds")

    kategori = ambil_kategori(key)
    videos = ambil_trending(key)

    os.makedirs(DATA_DIR, exist_ok=True)
    out_path = os.path.join(DATA_DIR, f"trending_{trending_date}.csv")
    # utf-8-sig supaya judul berhuruf non-latin/emoji terbaca benar di Excel
    with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=KOLOM)
        writer.writeheader()
        for rank, item in enumerate(videos, start=1):
            writer.writerow(ke_baris(rank, item, kategori, trending_date, fetched_at))

    print(f"[{fetched_at}] {len(videos)} video tersimpan ke {out_path}")


if __name__ == "__main__":
    main()
```

### #07 – Jadwal ambil data harian (3–4 minggu)

Script harus dijalankan **setiap hari pada jam yang sama**, misalnya jam 12.00 WIB, selama 3–4 minggu.

**Windows – Task Scheduler**

Buat file `jalankan_harian.bat` di folder yang sama dengan script:

```bat
@echo off
REM Dipanggil oleh Windows Task Scheduler sekali sehari.
REM Output dan error dicatat ke log.txt sebagai bukti pengambilan data.
cd /d "%~dp0"
python ambil_trending.py >> log.txt 2>&1
```

Daftarkan ke Task Scheduler lewat Command Prompt. Ganti path sesuai lokasi folder:

```bash
schtasks /create /tn "TrendingYouTubeID" /tr "\"C:\path\ke\folder\jalankan_harian.bat\"" /sc daily /st 12:00
```

**Linux / macOS – cron**

Jalankan `crontab -e`, lalu tambahkan baris berikut:

```bash
0 12 * * * cd /path/ke/folder && python3 ambil_trending.py >> log.txt 2>&1
```

**Catatan penting**

- Task Scheduler dan cron hanya jalan kalau komputernya menyala. API hanya memberi daftar trending **saat itu juga**, jadi hari yang terlewat tidak bisa diambil lagi.
- Supaya aman, minta **2 anggota** menjalankan script yang sama sebagai cadangan. Kalau datanya ganda, dibersihkan di tahap #10.
- Google Colab **tidak bisa** dijadwalkan otomatis. Kalau pakai Colab, harus dijalankan manual setiap hari.
- `log.txt` bisa dipakai sebagai bukti bahwa pengambilan data jalan setiap hari.

**Bukti selesai:** folder `data/` berisi file CSV harian selama 3–4 minggu.

### #07 (alternatif) – Jadwal harian di VPS dengan Coolify

VPS menyala 24 jam, jadi lebih andal daripada laptop. Ada dua cara.

#### Cara A – Lewat Coolify (data bisa diunduh lewat browser)

Repo: <https://github.com/AbdurRouf05/trending-youtube-indonesia>. Struktur repo:

```
/
├── Dockerfile
├── .dockerignore
├── .gitignore
├── README.md
├── Kanban_Trending_YouTube.pdf
├── Panduan_Pengumpulan_Data.md
└── pengumpulan_data/
    ├── ambil_trending.py
    ├── jalankan_harian.bat
    ├── api_key.txt      (tidak di-commit)
    └── data/            (tidak di-commit)
```

`Dockerfile` (di root repo, jadi Base directory Coolify tetap `/`):

```dockerfile
# Container untuk Coolify:
# - Proses utama: file server sederhana supaya CSV di /app/data bisa diunduh lewat browser.
# - Pengambilan data dijalankan oleh Scheduled Task Coolify: python ambil_trending.py
FROM python:3.12-slim

ENV TZ=Asia/Jakarta \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY pengumpulan_data/ambil_trending.py .
RUN mkdir -p data

EXPOSE 8000
CMD ["python", "-m", "http.server", "8000", "--directory", "data"]
```

`.gitignore`, supaya API key dan data tidak ikut ter-upload:

```
api_key.txt
pengumpulan_data/data/
log.txt
__pycache__/
```

`.dockerignore`, supaya image hanya berisi script:

```
*
!pengumpulan_data/ambil_trending.py
```

Langkah-langkahnya:

1. **Upload ke GitHub.** Push seluruh folder proyek ke repo. Pastikan `api_key.txt` **tidak** ikut. `.gitignore` sudah mengaturnya.
2. **Buat resource di Coolify:** pilih **+ New → Public Repository**, tempel URL repo, branch `main`.
   - Build Pack: **Dockerfile**
   - Ports Exposes: `8000`
   - Domain: isi kalau ingin CSV bisa diunduh lewat browser, misalnya `https://trending.domainkamu.com`. Kosongkan kalau tidak perlu.
3. **Environment Variables:** tambahkan `YOUTUBE_API_KEY` = *(API key kamu)*. Script otomatis membaca variable ini, jadi file `api_key.txt` tidak diperlukan.
4. **Persistent Storage (WAJIB):** tambahkan volume dengan destination path `/app/data`. Tanpa volume ini, **semua CSV hilang** setiap kali container di-redeploy.
5. Klik **Deploy**.
6. **Scheduled Tasks:** di halaman aplikasi, buka tab **Scheduled Tasks → + Add**.
   - Name: `ambil-trending`
   - Command: `python ambil_trending.py`
   - Frequency: `0 12 * * *` (setiap hari jam 12.00). Jam ini mengikuti zona waktu server di Coolify (**Servers → General → Server Timezone**). Kalau server masih UTC, pakai `0 5 * * *` supaya jalan jam 12.00 WIB.
   - Container: biarkan default, yaitu container aplikasi ini.
7. **Tes sekarang:** jalankan `python ambil_trending.py` sekali lewat tab **Terminal** di Coolify. Setelah itu buka domainnya. File `trending_YYYY-MM-DD.csv` harus muncul di daftar.

Cek setiap hari: riwayat dan output setiap eksekusi terlihat di tab Scheduled Tasks. Daftar file di domain juga harus bertambah satu setiap hari.

> Data YouTube trending bersifat publik, jadi tidak masalah kalau bisa diakses lewat domain. Namun jangan pernah taruh API key di folder `data/`.

**Status deploy (23 Okt 2026):**

- Aplikasi berjalan di Coolify (server `localhost`, timezone **UTC**).
- Scheduled task `ambil-trending` dengan jadwal `0 5 * * *` jalan setiap hari jam 12.00 WIB. Tes pertama sukses.
- Data tersimpan di volume Docker yang di-mount ke `/app/data`, dan bisa diunduh di <http://nbclm0mnvzce3mofyadgo76d.147.93.81.45.sslip.io>.
- Data **tidak** otomatis masuk ke GitHub. Setelah 3–4 minggu, unduh semua CSV dari link di atas untuk tahap #09.

#### Cara B – Cron langsung di VPS (paling sederhana, tanpa Coolify)

Masuk ke VPS lewat SSH, lalu jalankan:

```bash
mkdir -p ~/trending && cd ~/trending
# upload ambil_trending.py ke folder ini (scp / git clone / nano)
echo "API_KEY_KAMU" > api_key.txt
chmod 600 api_key.txt
python3 ambil_trending.py        # tes sekali
crontab -e
```

Tambahkan baris berikut. `0 5` UTC sama dengan 12.00 WIB. Cek zona waktu server dengan `timedatectl`.

```bash
0 5 * * * cd ~/trending && python3 ambil_trending.py >> log.txt 2>&1
```

Untuk mengambil data ke laptop, misalnya saat tahap #09:

```bash
scp -r user@ip-vps:~/trending/data ./data
```

### #08 – Cari dataset pendukung (Kaggle)

- Cari **"YouTube Trending Video Dataset"** di Kaggle.
- Dataset yang populer di sana umumnya **tidak memuat Indonesia**. Negaranya antara lain India, Amerika, Inggris, Jepang, dan Korea. Jadi pakai sebagai **pembanding**, misalnya Indonesia dibandingkan dengan India.
- Kolomnya mirip dengan CSV dari script (title, channel, category, publish time, tags, views, likes, comments), jadi mudah digabung.

**Bukti selesai:** dataset terunduh.

### #09 – Simpan data ke storage

1. Setelah 3–4 minggu, unggah seluruh folder `data/` ke Google Drive bersama atau ke HDFS.
2. Gabungkan semua file harian di PySpark:

```python
df = spark.read.csv(
    "data/*.csv",
    header=True,
    multiLine=True,   # deskripsi bisa berisi baris baru
    escape='"',
    inferSchema=True,
)
```

**Bukti selesai:** satu dataset terpusat.

---

## 3. Catatan untuk tahap berikutnya

- **Lama trending** (#11) dihitung dari berapa hari sebuah `video_id` muncul di file harian. Karena itu pengambilan data harus rutin setiap hari.
- **Durasi** (#10) masih berformat ISO 8601 (`PT1H2M30S`) dan perlu diubah ke detik.
- **Jam upload** (#11): `publish_time` dalam UTC. Tambahkan 7 jam untuk WIB.
- **Tag** dipisah dengan `|`. Jumlah tag dihitung dari hasil `split('|')`.
- Data **dislike tidak tersedia** lagi dari API sejak 2021, jadi jangan dimasukkan ke rumusan masalah.
- Video yang sama bisa muncul di banyak hari. Untuk modeling (#13–#14), tentukan apakah dipakai data hari terakhir per video atau semua baris.

---

## 4. Aturan Kanban dan pengumpulan tugas (dari PDF)

**Tata letak kertas**

- Kertas karton atau manila ukuran A2/A3, posisi landscape.
- 4 kolom: **Backlog** (paling lebar), **To Do**, **Doing**, **Done**.
- Judul proyek dan nama anggota di bagian atas kertas, legenda warna di pojok bawah.

**Isi sticky note**

- **Nomor** di pojok kanan atas.
- **Judul** ditulis besar dengan spidol.
- **PIC** (penanggung jawab) dan **deadline** di bagian bawah.

**Aturan pakai**

- Maksimal 2–3 sticky note di kolom **Doing** sekaligus.
- Pindahkan sticky note setiap pertemuan, lalu foto papannya sebagai bukti progres.
- Note yang selesai boleh diberi centang ✔ sebelum dipindah ke Done.
- Tugas baru ditambahkan ke Backlog dengan nomor berikutnya (#21, #22, …).

**Pengumpulan tugas**

- Foto bagian **Backlog** dari dekat, dengan cahaya terang dan tanpa pantulan.
- Masukkan foto ke Word bersama nama anggota dan judul proyek.
