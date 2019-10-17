import logging

logger = logging.getLogger(__name__)

# Path to CZO CMS export (csv file)
CZO_DATA_CSV = "./data/czo-2019-10-17.csv"

# a list of czo_ids to migrate.
# If empty or None, list to be generated automatically by START_ROW_INDEX and END_ROW_INDEX below
CZO_ID_LIST_TO_MIGRATE = []

# only work when CZO_ID_LIST_TO_MIGRATE is empty or NONE
START_ROW_INDEX = 0  # start row index in CZO_DATA_CSV
END_ROW_INDEX = 10 #434  # end row index in CZO_DATA_CSV (may change with new czo.csv)

# Migration logs
LOG_DIR = "./logs"
CLEAR_LOGS = False  # delete everything in the LOG_DIR

# REST API url
HS_URL = "localhost"  # localhost; dev-hs-6.cuahsi.org
PORT = "8000"  # https: 443
USE_HTTPS = False
VERIFY_HTTPS = False  # check if HTTPS certificate is valid
# external-accessible url for map preview
HS_EXTERNAL_FULL_DOMAIN = "http://localhost:8000"  # eg: https://www.hydroshare.org

# Mapping of czo names, hydroshare groups, hydroshare users, etc
# czo:  czo name used in CZO CMS (case-Insensitive)
# group: HydroShare Group name (case-sensitive)
# uname: HydroShare Username (case-sensitive)
# pwd: HydroShare Password (case-sensitive)
CZO_ACCOUNTS = [
    {"czo":  "national", "group": "CZO National", "uname": "czo_national", "pwd": "123"},
    {"czo":  "boulder", "group": "CZO Boulder", "uname": "czo_boulder", "pwd": "123"},
    {"czo":  "eel", "group": "CZO Eel", "uname": "czo_eel", "pwd": "123"},
    {"czo":  "catalina-jemez", "group": "CZO Catalina-Jemez", "uname": "czo_catalina-jemez", "pwd": "123"},
    {"czo":  "reynolds", "group": "CZO Reynolds", "uname": "czo_reynolds", "pwd": "123"},
    {"czo":  "luquillo", "group": "CZO Luquillo", "uname": "czo_luquillo", "pwd": "123"},
    {"czo":  "sierra", "group": "CZO Sierra", "uname": "czo_sierra", "pwd": "123"},
    {"czo":  "christina", "group": "CZO Christina", "uname": "czo_christina", "pwd": "123"},
    {"czo":  "shale hills", "group": "CZO Shale-Hills", "uname": "czo_shale-hills", "pwd": "123"},
    {"czo":  "calhoun", "group": "CZO Calhoun", "uname": "czo_calhoun", "pwd": "123"},
    # Keep the last line unchanged
    {"czo":  "default", "group": "", "uname": "czo", "pwd": "123"},
]

# file size above this limit to be migrated as reference types
BIG_FILE_SIZE_MB = 500

# http headers (Do not change)
headers = {
    'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36"
}

# Use predownload files
USE_CACHED_FILES = True
# Path to predownloaded files
CACHED_FILE_DIR = "./tmp"

# Unit conversion
MB_TO_BYTE = 1024 * 1024

# ReadMe filename
README_FILENAME = "ReadMe.md"
README_COLUMN_MAP_PATH = './data/markdown_map.json'
README_SHOW_MAPS = True

# Switch to activate 2nd pass (keep True)
RUN_2ND_PASS = True


## Keep Codes Below Unchanged ##
# local_settings overriding settings
try:
    from local_settings import *
except ImportError:
    print("----ERROR OVERRIDING SETTINGS WITH LOCAL_SETTINGS-------")

# append hydroshare connection info to each czo account
connection_info = {"hs_url": HS_URL, "port": PORT, "use_https": USE_HTTPS, "verify_https": VERIFY_HTTPS}
for i in CZO_ACCOUNTS:
    i.update(connection_info)
