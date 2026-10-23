@echo off
REM Dipanggil oleh Windows Task Scheduler sekali sehari.
REM Output dan error dicatat ke log.txt sebagai bukti pengambilan data.
cd /d "%~dp0"
python ambil_trending.py >> log.txt 2>&1
