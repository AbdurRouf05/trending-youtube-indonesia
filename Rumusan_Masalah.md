# Rumusan Masalah (Kartu #04)

**Judul:** Analisis Data Trending YouTube Indonesia untuk Mengidentifikasi Faktor Penentu Popularitas Video Menggunakan Apache Spark

## Latar belakang singkat

YouTube adalah salah satu platform video paling banyak digunakan di Indonesia. Setiap hari YouTube menampilkan daftar video terpopuler (*mostPopular*) per negara. Daftar ini menunjukkan konten apa yang sedang paling banyak ditonton. Sampai sekarang belum jelas karakteristik apa yang membuat sebuah video masuk daftar tersebut dan bertahan lama di sana. Contohnya kategori, waktu upload, durasi, judul, dan tag.

Data dikumpulkan sendiri setiap hari selama 3–4 minggu melalui YouTube Data API v3 (`chart=mostPopular`, `regionCode=ID`). Hasilnya kurang lebih 200 video per hari. Data kemudian diolah dengan Apache Spark (PySpark dan Spark MLlib).

## Pertanyaan penelitian

1. **Kategori apa yang paling mendominasi video trending di Indonesia, dan kategori mana yang menghasilkan views serta engagement (rasio like/view) tertinggi?**
   - Fitur: `category_name`, `view_count`, `rasio_like_view`
   - Analisis: agregasi jumlah video, median views, dan rata-rata engagement per kategori

2. **Kapan waktu upload (jam dan hari) yang paling sering menghasilkan video trending?**
   - Fitur: `jam_upload` (WIB), `hari_upload`, `upload_akhir_pekan`
   - Analisis: distribusi jumlah video dan median views per jam/hari

3. **Apakah durasi video, panjang judul, dan jumlah tag berpengaruh terhadap jumlah views dan lama video bertahan di trending?**
   - Fitur: `durasi_menit`, `panjang_judul`, `jumlah_tag`, `lama_trending_hari`
   - Analisis: perbandingan kelompok durasi, korelasi, dan perbandingan video populer dengan tidak populer

4. **Seberapa cepat sebuah video masuk trending setelah diupload, dan berapa lama biasanya video bertahan di daftar trending?**
   - Fitur: `jam_sampai_trending`, `lama_trending_hari`, `rank_terbaik`
   - Analisis: statistik deskriptif dan perbandingan antar kategori

5. **Apakah popularitas video (populer atau tidak populer) dapat diprediksi dari karakteristik video, dan faktor apa yang paling menentukan?**
   - Label: `populer` = 1 jika views di atas persentil 75
   - Model: Random Forest dan Logistic Regression (Spark MLlib)
   - Evaluasi: akurasi, precision, recall, F1-score, dan *feature importance*

## Tujuan

1. Mengidentifikasi kategori dan karakteristik konten yang mendominasi trending YouTube Indonesia.
2. Menentukan waktu upload yang paling berpeluang menghasilkan video trending.
3. Mengukur pengaruh durasi, judul, dan tag terhadap popularitas video.
4. Membangun model klasifikasi popularitas video dengan Spark MLlib dan menemukan faktor yang paling berpengaruh.

## Batasan masalah

- Data yang dipakai hanya video dari daftar `chart=mostPopular` untuk wilayah Indonesia (`regionCode=ID`), diambil sekali sehari pukul 12.00 WIB selama 3–4 minggu.
- YouTube menghentikan halaman "Trending" publik pada Juli 2025. Dalam penelitian ini, istilah "trending" berarti daftar video terpopuler (*mostPopular*) yang tersedia melalui YouTube Data API v3.
- Jumlah dislike tidak tersedia, karena YouTube menyembunyikannya dari API sejak Desember 2021.
- Views, like, dan komentar adalah nilai saat data diambil, bukan nilai akhir video.
- Label "populer" bersifat relatif terhadap video lain di dataset (persentil 75), bukan ukuran popularitas absolut.
- Video yang sudah lama diupload cenderung punya views lebih besar. Karena itu fitur `jam_sampai_trending` ikut dipertimbangkan saat interpretasi.

## Manfaat

- **Kreator konten:** acuan kapan sebaiknya upload, durasi yang efektif, dan kategori yang sedang diminati.
- **Akademik:** contoh penerapan alur Big Data dengan Apache Spark, mulai dari pengumpulan data dari API, pemrosesan, analisis, sampai pemodelan.
