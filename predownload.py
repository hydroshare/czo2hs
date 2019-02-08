# This is a standalone script to prototype the pre-downloading feature
import os
import uuid
import logging
from datetime import datetime as dt

import pandas as pd
import validators

from utils import safe_get, retry_func


def _download(url, save_to_path):

    response = safe_get(url, stream=True)
    if len(response["error"]) > 0:
        raise Exception(response["error"])

    with open(save_to_path, 'wb') as f:
        f.write(response["content"])
    return os.path.getsize(save_to_path)


def _save_to_file(url, prefix="tmp"):
    if not validators.url(url):
        return

    rand_name = uuid.uuid4().hex[:6]
    fn = "{prefix}_{rand_name}".format(prefix=prefix, rand_name=rand_name)
    f_path = os.path.join(output_dir, fn)
    if url not in url_file_dict:

        size = retry_func(_download, args=[url, f_path])
        f_dict = {"path": f_path, "size": size, "url": url}
        logging.info("{}".format(f_dict))
        url_file_dict[url] = f_dict


def download_czo(czo_id):

        row_dict = _extract_data_row_as_dict(czo_id)
        files = row_dict['COMPONENT_FILES-location$topic$url$data_level$private$doi$metadata_url']

        for f_str in files.split("|"):
            f_info_list = f_str.split("$")
            f_url = f_info_list[2]
            f_metadata_url = f_info_list[6]

            try:
                _save_to_file(f_url, "f")
            except Exception as ex:
                logging.error(ex)

            try:
                _save_to_file(f_metadata_url, "meta")
            except Exception as ex:
                logging.error(ex)


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
    output_dir = create_output_dir()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)-5.5s]  %(message)s",
        handlers=[
            logging.FileHandler(os.path.join(output_dir, "./log/log_{}.log".format(start_time_str))),
            logging.StreamHandler()
        ])

    # read in czo.csv
    czo_df = pd.read_csv("data/czo.csv")
    czo_id_list = get_czo_id_list()

    url_file_dict = dict()
    # N = len(czo_id_list)
    N = 2
    for i in range(N):
        czo_id = czo_id_list[i]
        logging.info("Downloading files for czo_id {} ({}/{})".format(czo_id, i+1, N))
        download_czo(czo_id)
        print(url_file_dict)
    file_info_list = list(url_file_dict.values())

    df_lookup = pd.DataFrame(file_info_list)

    df_lookup.to_csv(os.path.join(output_dir, "./log/lookup_{}.csv".format(start_time_str)), index=False)
    logging.info("Total number {}; Total size (MB): {}".format(df_lookup["size"].count(),
                                                                df_lookup["size"].sum()/1024.0/1024.0))

    logging.info("Done")
