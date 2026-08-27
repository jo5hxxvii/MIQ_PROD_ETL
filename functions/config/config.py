import os

# ── CONFIG ────────────────────────────────────────────────────
SFTP_HOST = os.environ.get("DLD_SFTP_HOST")
SFTP_PORT = int(os.environ.get("DLD_SFTP_PORT", "22"))
SFTP_USERNAME = os.environ.get("DLD_SFTP_USERNAME")
SFTP_PASSWORD = os.environ.get("DLD_SFTP_PASSWORD")
SFTP_KEY_PATH = os.environ.get("DLD_SFTP_KEY_PATH", None)
SFTP_FOLDER = os.environ.get("DLD_SFTP_FOLDER", "/uploads")

PG_URL = os.environ.get("DLD_PG_URL")

# Slack webhook — set via environment variable
# export DLD_SLACK_WEBHOOK="https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
# Leave empty to disable Slack notifications silently
SLACK_WEBHOOK = os.environ.get("DLD_SLACK_WEBHOOK")

LOCAL_DOWNLOAD_FOLDER = "./dld_downloads"
BACKUP_SCHEMA_COUNT = 3
CHUNK_SIZE = 20000
DATE_KEYWORDS = ["at", "date", "time", "timestamp", "created", "updated"]
AREA_NICKNAMES_PATH = "./area_names.json"
GMAP_API_KEY = ""
OPENAI_API_KEY = ""
BLOCKING_CLIENT_IP = ""
