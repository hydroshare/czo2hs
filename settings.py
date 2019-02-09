import logging

logger = logging.getLogger(__name__)

headers = {
    'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36"
}

BIG_FILE_SIZE_MB = 500

LOG_DIR = "./logs"

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

# What CZO data to migrate
# PROCESS_FIRST_N_ROWS = 0  # N>0: process the first N rows in file "czo.csv"; N=0:all rows; N<0: a specific list of czo_id see CZO_ID_LIST

# READ_CZO_ID_LIST_FROM_CSV = False  # replace CZO_ID_LIST by reading a lsit of czo_id from file "czo_hs_id.csv"
# FIRST_N_ITEM_IN_CSV = 0  # process the first N items in CZO_ID_LIST; 0-all items;

STOP_AFTER = 2  # max number of rows to process; if you do anything higher than 399 that implies all, since there's only 399 total

