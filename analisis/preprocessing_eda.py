"""
Tahap #10–#13: cleaning, feature engineering, EDA, dan labeling popularitas dengan PySpark.

Input : pengumpulan_data/data/trending_*.csv  (hasil harian ambil_trending.py)
Output: analisis/output/fitur_video.csv       (satu baris per video, siap untuk training model #14)
        analisis/output/eda_*.csv             (tabel ringkasan EDA)
        analisis/output/grafik_*.png          (grafik EDA)

Jalankan dari mana saja (butuh Java 17+):
    pip install pyspark matplotlib
    python analisis/preprocessing_eda.py
"""

import glob
import os
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT = os.path.join(ROOT, "pengumpulan_data", "data", "trending_*.csv")
OUTPUT = os.path.join(ROOT, "analisis", "output")
# Batas label "populer" (#13): views di atas persentil 75
PERSENTIL_POPULER = 0.75
NAMA_HARI = ["Minggu", "Senin", "Selasa", "Rabu", "Kamis", "Jumat", "Sabtu"]


def buat_spark():
    # Pakai Python yang sama untuk worker Spark (di Windows, tanpa ini Spark bisa macet)
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    spark = (
        SparkSession.builder.appName("TrendingYouTubeID")
        .master("local[*]")
        # Semua jam (jam upload, dsb.) dihitung dalam WIB
        .config("spark.sql.session.timeZone", "Asia/Jakarta")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def baca_data(spark):
    # Daftar file diambil di Python, bukan wildcard Spark: di Windows tanpa winutils,
    # Spark gagal saat me-list isi folder
    files = sorted(glob.glob(INPUT))
    if not files:
        sys.exit(f"Tidak ada file di {INPUT}")
    df = spark.read.csv(files, header=True, multiLine=True, escape='"', encoding="UTF-8")
    # File ditulis dengan utf-8-sig, jadi kolom pertama bisa diawali BOM
    return df.toDF(*[c.lstrip("﻿") for c in df.columns])


def ke_angka(col, tipe="long"):
    """Cast teks ke angka; string kosong jadi NULL (Spark 4 menolak cast '' secara default)."""
    col = F.col(col) if isinstance(col, str) else col
    return F.when(F.trim(col) != "", col.cast(tipe))


def cleaning(df):
    """#10: tipe data, hapus duplikat, format durasi/tanggal/tag."""
    df = (
        df.withColumn("rank", ke_angka("rank", "int"))
        .withColumn("view_count", ke_angka("view_count"))
        .withColumn("like_count", ke_angka("like_count"))
        .withColumn("comment_count", ke_angka("comment_count"))
        .withColumn("comments_disabled", F.col("comments_disabled") == "True")
        .withColumn("publish_ts", F.to_timestamp("publish_time"))
        .withColumn("fetched_ts", F.to_timestamp("fetched_at"))
        .withColumn("trending_date", F.to_date("trending_date"))
    )

    # Kalau satu hari terambil lebih dari sekali (mis. dari 2 mesin), ambil yang terakhir
    w = Window.partitionBy("video_id", "trending_date").orderBy(F.col("fetched_ts").desc())
    df = df.withColumn("_n", F.row_number().over(w)).filter("_n = 1").drop("_n")

    # Durasi ISO 8601 (mis. P1DT2H3M4S, PT9M50S, P0D untuk live) -> detik
    def bagian(pola):
        return F.coalesce(ke_angka(F.regexp_extract("duration", pola, 1), "int"), F.lit(0))

    detik = (
        bagian(r"P(\d+)D") * 86400
        + bagian(r"T.*?(\d+)H") * 3600
        + bagian(r"T.*?(\d+)M") * 60
        + bagian(r"T.*?(\d+)S")
    )
    df = df.withColumn("durasi_detik", detik)

    tags = F.when(F.col("tags").isNull() | (F.col("tags") == ""), F.array()).otherwise(
        F.split("tags", r"\|")
    )
    return df.withColumn("tag_list", tags).filter(F.col("view_count").isNotNull())


def feature_engineering(df):
    """#11: satu baris per video, memakai snapshot terakhir + agregat selama trending."""
    w_akhir = Window.partitionBy("video_id").orderBy(F.col("trending_date").desc())
    akhir = df.withColumn("_n", F.row_number().over(w_akhir)).filter("_n = 1").drop("_n")

    agregat = df.groupBy("video_id").agg(
        F.countDistinct("trending_date").alias("lama_trending_hari"),
        F.min("rank").alias("rank_terbaik"),
        F.min("trending_date").alias("tanggal_pertama_trending"),
        F.min("fetched_ts").alias("pertama_terlihat_ts"),
    )

    v = akhir.join(agregat, "video_id")
    views = F.col("view_count")
    return v.select(
        "video_id", "title", "channel_title", "category_name",
        "publish_ts", "tanggal_pertama_trending",
        F.hour("publish_ts").alias("jam_upload"),
        F.element_at(F.array(*[F.lit(h) for h in NAMA_HARI]), F.dayofweek("publish_ts")).alias("hari_upload"),
        F.dayofweek("publish_ts").isin(1, 7).cast("int").alias("upload_akhir_pekan"),
        "durasi_detik",
        F.round(F.col("durasi_detik") / 60, 2).alias("durasi_menit"),
        F.length("title").alias("panjang_judul"),
        F.size(F.split(F.trim("title"), r"\s+")).alias("jumlah_kata_judul"),
        F.size("tag_list").alias("jumlah_tag"),
        F.length(F.coalesce("description", F.lit(""))).alias("panjang_deskripsi"),
        (F.col("definition") == "hd").cast("int").alias("is_hd"),
        (F.col("caption") == "true").cast("int").alias("ada_caption"),
        F.col("comments_disabled").cast("int").alias("komentar_dimatikan"),
        # Jam sejak upload sampai pertama kali masuk trending
        F.round(
            (F.col("pertama_terlihat_ts").cast("long") - F.col("publish_ts").cast("long")) / 3600, 1
        ).alias("jam_sampai_trending"),
        "lama_trending_hari", "rank_terbaik",
        "view_count", "like_count", "comment_count",
        F.when(views > 0, F.col("like_count") / views).alias("rasio_like_view"),
        F.when(views > 0, F.col("comment_count") / views).alias("rasio_komentar_view"),
    )


def labeling(fitur):
    """#13: populer = views di atas persentil 75."""
    batas = fitur.approxQuantile("view_count", [PERSENTIL_POPULER], 0.0)[0]
    return fitur.withColumn("populer", (F.col("view_count") >= batas).cast("int")), batas


def kategori_durasi(col):
    return (
        F.when(col == 0, "live/0")
        .when(col < 240, "< 4 menit")
        .when(col < 600, "4–10 menit")
        .when(col < 1200, "10–20 menit")
        .when(col < 3600, "20–60 menit")
        .otherwise("> 60 menit")
    )


def eda(df, fitur):
    """#12: statistik deskriptif dan tabel ringkasan."""
    print("\n=== Ringkasan data ===")
    rentang = df.agg(F.min("trending_date"), F.max("trending_date")).first()
    print(f"Baris snapshot : {df.count()}")
    print(f"Video unik     : {fitur.count()}")
    print(f"Rentang tanggal: {rentang[0]} s/d {rentang[1]} "
          f"({df.select('trending_date').distinct().count()} hari)")

    print("\n=== Statistik deskriptif (per video) ===")
    fitur.select(
        "view_count", "like_count", "comment_count", "durasi_menit",
        "panjang_judul", "jumlah_tag", "rasio_like_view", "lama_trending_hari",
    ).summary("count", "mean", "min", "25%", "50%", "75%", "max").show(truncate=False)

    median = F.percentile_approx("view_count", 0.5)
    tabel = {
        "kategori": fitur.groupBy("category_name").agg(
            F.count("*").alias("jumlah_video"), median.alias("median_views"),
            F.round(F.avg("rasio_like_view"), 4).alias("rata_like_per_view"),
            F.round(F.avg("lama_trending_hari"), 2).alias("rata_lama_trending"),
        ).orderBy(F.desc("jumlah_video")),
        "jam_upload": fitur.groupBy("jam_upload").agg(
            F.count("*").alias("jumlah_video"), median.alias("median_views"),
        ).orderBy("jam_upload"),
        "hari_upload": fitur.groupBy("hari_upload").agg(
            F.count("*").alias("jumlah_video"), median.alias("median_views"),
        ).orderBy(F.desc("jumlah_video")),
        "durasi": fitur.withColumn("kelompok_durasi", kategori_durasi(F.col("durasi_detik")))
        .groupBy("kelompok_durasi").agg(
            F.count("*").alias("jumlah_video"), median.alias("median_views"),
            F.min("durasi_detik").alias("_urut"),
        ).orderBy("_urut").drop("_urut"),
        "channel_teratas": fitur.groupBy("channel_title").agg(
            F.count("*").alias("jumlah_video"), F.sum("view_count").alias("total_views"),
        ).orderBy(F.desc("jumlah_video")).limit(15),
        "label": fitur.groupBy("populer").agg(
            F.count("*").alias("jumlah_video"),
            F.round(F.avg("durasi_menit"), 1).alias("rata_durasi_menit"),
            F.round(F.avg("jumlah_tag"), 1).alias("rata_jumlah_tag"),
            F.round(F.avg("panjang_judul"), 1).alias("rata_panjang_judul"),
            F.round(F.avg("rasio_like_view"), 4).alias("rata_like_per_view"),
        ).orderBy("populer"),
    }
    hasil = {}
    for nama, t in tabel.items():
        print(f"\n=== {nama} ===")
        pdf = t.toPandas()
        print(pdf.to_string(index=False))
        pdf.to_csv(os.path.join(OUTPUT, f"eda_{nama}.csv"), index=False, encoding="utf-8-sig")
        hasil[nama] = pdf
    return hasil


def grafik(hasil):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("\n(matplotlib tidak terpasang, grafik dilewati)")
        return

    def simpan(nama, x, y, judul, xlabel, ylabel, horizontal=False):
        fig, ax = plt.subplots(figsize=(9, 5))
        (ax.barh if horizontal else ax.bar)(x, y, color="#3b6fb6")
        ax.set_title(judul)
        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)
        if horizontal:
            ax.invert_yaxis()
        fig.tight_layout()
        fig.savefig(os.path.join(OUTPUT, f"grafik_{nama}.png"), dpi=120)
        plt.close(fig)

    k = hasil["kategori"]
    simpan("kategori", k["category_name"], k["jumlah_video"],
           "Jumlah video trending per kategori", "Jumlah video", "", horizontal=True)
    j = hasil["jam_upload"]
    simpan("jam_upload", j["jam_upload"].astype(str), j["jumlah_video"],
           "Jam upload video trending (WIB)", "Jam", "Jumlah video")
    d = hasil["durasi"]
    simpan("durasi", d["kelompok_durasi"], d["median_views"],
           "Median views per kelompok durasi", "Durasi", "Median views")


def main():
    os.makedirs(OUTPUT, exist_ok=True)
    spark = buat_spark()

    df = cleaning(baca_data(spark)).cache()
    fitur, batas = labeling(feature_engineering(df))
    fitur = fitur.cache()
    print(f"\nBatas populer (persentil {int(PERSENTIL_POPULER * 100)}): {batas:,.0f} views")

    hasil = eda(df, fitur)
    grafik(hasil)

    # Tabel fitur kecil (ribuan baris), jadi cukup ditulis lewat pandas;
    # ini juga menghindari kebutuhan winutils saat Spark menulis file di Windows.
    path = os.path.join(OUTPUT, "fitur_video.csv")
    fitur.toPandas().to_csv(path, index=False, encoding="utf-8-sig")
    print(f"\nTabel fitur tersimpan: {path}")
    spark.stop()


if __name__ == "__main__":
    main()
