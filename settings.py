import logging

logger = logging.getLogger(__name__)

STOP_AFTER = 10  # max number of rows to process; if you do anything higher than 399 that implies all

LOG_DIR = "./logs"
CLEAR_LOGS = True  # delete everything in the LOG_DIR

HS_URL = "localhost"  # dev-hs-6.cuahsi.org

CZO_ACCOUNTS = {
    "default": {"uname": "czo", "pwd": "123", "hs_url": HS_URL},
    # "national": {"uname": "czo_national", "pwd": "123", "hs_url": HS_URL},
    # "boulder": {"uname": "czo_boulder", "pwd": "123", "hs_url": HS_URL},
    # "eel": {"uname": "czo_eel", "pwd": "123", "hs_url": HS_URL},
    # "catalina-jemez": {"uname": "czo_catalina-jemez", "pwd": "123", "hs_url": HS_URL},
    # "reynolds": {"uname": "czo_reynolds", "pwd": "123", "hs_url": HS_URL},
    # "luquillo": {"uname": "czo_luquillo", "pwd": "123", "hs_url": HS_URL},
}

BIG_FILE_SIZE_MB = 500

headers = {
    'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36"
}
