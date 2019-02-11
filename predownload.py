# This is a standalone script to prototype the pre-downloading feature
import os
import logging
from datetime import datetime as dt
import hashlib
from multiprocessing import Manager, Pool

import requests
import pandas as pd
import validators

from util import retry_func
from settings import headers, MB_TO_BYTE

requests.packages.urllib3.disable_warnings()
N_PROCESS = 4


def _hash_string(_str):

    hash_object = hashlib.md5(_str.encode())
    return hash_object.hexdigest()


def _download(url, save_to_path):

    # sending headers is very important or in some cases requests.get() wont download the actual file content/binary
    response = requests.get(url, stream=True, verify=False, headers=headers)

    with open(save_to_path, 'wb') as fd:
        for chunk in response.iter_content(chunk_size=5*MB_TO_BYTE):
            fd.write(chunk)

    return os.path.getsize(save_to_path)


def _save_to_file(url):
    if not validators.url(url):
        return
    url_hash = _hash_string(url)
    if url_hash not in url_file_dict:

        fn = "{fname}".format(fname=url_hash)
        f_path = os.path.join(output_dir, fn)

        logging.info("{}".format(url))
        size = retry_func(_download, args=[url, f_path])
        f_dict = {"url_md5": url_hash, "path": f_path, "size": size, "url": url}
        logging.info("Saved to {f_path}: {size_mb:0.4f} MB".format(f_path=f_path, size_mb=float(size)/MB_TO_BYTE))
        url_file_dict[url_hash] = f_dict


def download_czo(czo_id):
        logging.info("Downloading files for czo_id {}".format(czo_id))
        row_dict = _extract_data_row_as_dict(czo_id)
        files = row_dict['COMPONENT_FILES-location$topic$url$data_level$private$doi$metadata_url']

        for f_str in files.split("|"):
            f_info_list = f_str.split("$")
            f_url = f_info_list[2]
            f_metadata_url = f_info_list[6]

            try:
                _save_to_file(f_url)
            except Exception as ex:
                logging.error(ex)

            try:
                _save_to_file(f_metadata_url)
            except Exception as ex:
                logging.error(ex)
        czo_id_done.append(czo_id)
        logging.info("Finished czo_ids: {}/{}".format(len(czo_id_done), len(czo_id_list_subset)))


def get_czo_id_list():
    return czo_df['czo_id'].values


def create_output_dir():

    log_dir = os.path.join(base_dir, start_time_str, "log")

    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    return os.path.dirname(log_dir)


def _extract_data_row_as_dict(czo_id):
    df_row = czo_df.loc[czo_df['czo_id'] == czo_id]
    czo_row_dict = df_row.to_dict(orient='records')[0]
    return czo_row_dict


if __name__ == "__main__":

    start_time = dt.utcnow()
    start_time_str = start_time.strftime("%Y-%m-%d_%H-%M-%S")

    # prepare output dir
    base_dir = "./tmp"
    if not os.path.isabs(base_dir):
        base_dir = os.path.abspath(base_dir)

    output_dir = create_output_dir()

    logging.basicConfig(
        level=logging.INFO,
        format="%(processName)s %(asctime)s [%(levelname)-5.5s]  %(message)s",
        handlers=[
            logging.FileHandler(os.path.join(output_dir, "./logs/log_{}.log".format(start_time_str))),
            logging.StreamHandler()
        ])

    # read in czo.csv
    czo_df = pd.read_csv("./data/czo.csv")
    czo_id_list = get_czo_id_list()
    #czo_id_list = [2464]
    czo_id_list_subset = czo_id_list[:]

    with Manager() as manager:

        url_file_dict = manager.dict()
        czo_id_done = manager.list()
        with Pool(N_PROCESS) as p:
            p.map(download_czo, czo_id_list_subset)

        file_info_list = list(url_file_dict.values())

        df_lookup = pd.DataFrame(file_info_list)

        df_lookup.to_csv(os.path.join(output_dir, "./log/lookup_{}.csv".format(start_time_str)), index=False)
        logging.info("Total number {}; Total size (MB): {}".format(df_lookup["size"].count(),
                                                                   df_lookup["size"].sum()/MB_TO_BYTE))

    logging.info("Done")
