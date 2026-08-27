import os

# ── CONFIG ────────────────────────────────────────────────────
SFTP_HOST = os.environ.get("DLD_SFTP_HOST", "144.126.255.45")
SFTP_PORT = int(os.environ.get("DLD_SFTP_PORT", "22"))
SFTP_USERNAME = os.environ.get("DLD_SFTP_USERNAME", "aiq")
SFTP_PASSWORD = os.environ.get("DLD_SFTP_PASSWORD", "fvaMkCsu3tboNotZcBB")
SFTP_KEY_PATH = os.environ.get("DLD_SFTP_KEY_PATH", None)
SFTP_FOLDER = os.environ.get("DLD_SFTP_FOLDER", "/uploads")

PG_URL = os.environ.get(
    "DLD_PG_URL",
    "postgresql://doadmin:AVNS_yH4rB22omDE4Q93wusT@private-miq-db-postgresql-v1-do-user-23625062-0.g.db.ondigitalocean.com:25060/defaultdb",
)

# Slack webhook — set via environment variable
# export DLD_SLACK_WEBHOOK="https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
# Leave empty to disable Slack notifications silently
SLACK_WEBHOOK = os.environ.get(
    "DLD_SLACK_WEBHOOK",
    "https://hooks.slack.com/services/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",
)

LOCAL_DOWNLOAD_FOLDER = "./dld_downloads"
BACKUP_SCHEMA_COUNT = 3
CHUNK_SIZE = 20000
DATE_KEYWORDS = ["at", "date", "time", "timestamp", "created", "updated"]
AREA_NICKNAMES_PATH = "./area_names.json"
GMAP_API_KEY = "AIzaSyBxvjy48-51gwZKLvS9AYbjoOm5zQTLGVI"
OPENAI_API_KEY = "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"
BLOCKING_CLIENT_IP = "68.183.93.23"
