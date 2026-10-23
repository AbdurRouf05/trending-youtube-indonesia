# Trending YouTube Indonesia – Pengumpulan Data

Script pengambil data video trending YouTube Indonesia untuk proyek mata kuliah Big Data (7ITA1):
*Analisis Data Trending YouTube Indonesia untuk Mengidentifikasi Faktor Penentu Popularitas Video Menggunakan Apache Spark.*

Data diambil dari YouTube Data API v3 (`videos.list`, `chart=mostPopular`, `regionCode=ID`) sekali sehari selama 3–4 minggu.
Setiap pengambilan disimpan sebagai `data/trending_YYYY-MM-DD.csv` (±200 video per hari).

## Isi repo

| File | Fungsi |
|---|---|
| `ambil_trending.py` | Script pengambil data (hanya library bawaan Python 3) |
| `jalankan_harian.bat` | Pemanggil script untuk Windows Task Scheduler |
| `Dockerfile` | Container untuk deploy di VPS (Coolify) + file server untuk mengunduh CSV |

## Menjalankan

1. Buat API key YouTube Data API v3 di Google Cloud Console.
2. Set environment variable `YOUTUBE_API_KEY`, atau buat file `api_key.txt` berisi key tersebut (file ini tidak ikut di-commit).
3. Jalankan:

```bash
python ambil_trending.py
```

## Kolom data

`trending_date`, `fetched_at`, `rank`, `video_id`, `title`, `channel_id`, `channel_title`, `category_id`, `category_name`,
`publish_time`, `tags` (dipisah `|`), `duration` (ISO 8601), `definition`, `caption`, `view_count`, `like_count`,
`comment_count`, `comments_disabled`, `thumbnail_link`, `description`.
