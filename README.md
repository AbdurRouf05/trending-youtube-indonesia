# Analisis Trending YouTube Indonesia

Proyek mata kuliah Big Data (7ITA1):
*Analisis Data Trending YouTube Indonesia untuk Mengidentifikasi Faktor Penentu Popularitas Video Menggunakan Apache Spark.*

Data diambil dari YouTube Data API v3 (`videos.list`, `chart=mostPopular`, `regionCode=ID`) sekali sehari selama 3–4 minggu.
Setiap pengambilan disimpan sebagai `trending_YYYY-MM-DD.csv` (±200 video per hari).

## Struktur repo

| Path | Isi |
|---|---|
| `Kanban_Trending_YouTube.pdf` | Kanban board proyek (20 tugas, Backlog → Done) |
| `Panduan_Pengumpulan_Data.md` | Panduan lengkap pengumpulan data (API key, script, jadwal, Coolify) |
| `pengumpulan_data/ambil_trending.py` | Script pengambil data (hanya library bawaan Python 3) |
| `pengumpulan_data/jalankan_harian.bat` | Pemanggil script untuk Windows Task Scheduler |
| `Dockerfile` | Container untuk deploy di VPS (Coolify) + file server untuk mengunduh CSV |

## Menjalankan

1. Buat API key YouTube Data API v3 di Google Cloud Console.
2. Set environment variable `YOUTUBE_API_KEY`, atau buat file `pengumpulan_data/api_key.txt` berisi key tersebut (file ini tidak ikut di-commit).
3. Jalankan:

```bash
python pengumpulan_data/ambil_trending.py
```

Hasil tersimpan di `pengumpulan_data/data/`.

## Kolom data

`trending_date`, `fetched_at`, `rank`, `video_id`, `title`, `channel_id`, `channel_title`, `category_id`, `category_name`,
`publish_time`, `tags` (dipisah `|`), `duration` (ISO 8601), `definition`, `caption`, `view_count`, `like_count`,
`comment_count`, `comments_disabled`, `thumbnail_link`, `description`.
