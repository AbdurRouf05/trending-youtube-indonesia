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
