import logging

logger = logging.getLogger(__name__)

headers = {
    'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36"
}

BIG_FILE_SIZE_MB = 500

LOG_DIR = "./log"

HS_URL = "localhost"  # dev-hs-6.cuahsi.org

CZO_ACCOUNTS = {
    "default": {"uname": "czo", "pwd": "123", "HS_URL": HS_URL},
    "national": {"uname": "czo_national", "pwd": "123", "HS_URL": HS_URL},
    "boulder": {"uname": "czo_boulder", "pwd": "123", "HS_URL": HS_URL},
    "eel": {"uname": "czo_eel", "pwd": "123", "HS_URL": HS_URL},
    "catalina-jemez": {"uname": "czo_catalina-jemez", "pwd": "123", "HS_URL": HS_URL},
    "reynolds": {"uname": "czo_reynolds", "pwd": "123", "HS_URL": HS_URL},
    "luquillo": {"uname": "czo_luquillo", "pwd": "123", "HS_URL": HS_URL},
}
