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
