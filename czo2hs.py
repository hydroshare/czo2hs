import logging
import os
from datetime import datetime as dt

import pandas as pd
from pandas.io.json import json_normalize

from accounts import CZOHSAccount
from utils import create_hs_res_from_czo_row, elapsed_time, log_progress, \
    log_uploaded_file_stats, get_czo_list_from_csv


def logging_init(_logdir):
    """
    Configure environment and logging settings
    :return:
    """
    if not os.path.exists(_logdir):
        os.makedirs(_logdir)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
        handlers=[
            logging.FileHandler(log_file_path),
            logging.StreamHandler()
        ])


def migrate_czo_row(czo_row_dict, progress_dict, czo_hs_id_lookup_df, row_no=1, progress_report=5):
    """
    Create a HS resource from a CZO row dict
    :param czo_row_dict: czo data row dict
    :param row_no: row number
    :param progress_report: print out progress report every N rows
    :return:
    """

    logging.info("=" * 80)
    dt_start_resource = dt.utcnow()
    logging.info("Start migrating one Resource at UTC {}".format(dt_start_resource))

    mgr_record_dict = create_hs_res_from_czo_row(czo_row_dict, CZO_HS_Account_Obj, index=row_no)

    if mgr_record_dict["success"]:
        progress_dict["success"].append(mgr_record_dict)
    else:
        progress_dict["error"].append(mgr_record_dict)

    # append czo_id vs hs_id to lookup table
    czo_hs_id_lookup_dict = {"czo_id": mgr_record_dict["czo_id"],
                             "hs_id": mgr_record_dict["hs_id"],
                             "success": mgr_record_dict["success"]
                             }

    # TODO can this be removed:
    czo_hs_id_lookup_df = czo_hs_id_lookup_df.append(czo_hs_id_lookup_dict, ignore_index=True)

    log_uploaded_file_stats(mgr_record_dict)

    elapsed_time(dt_start_resource, prompt_str="Resource Creation Time Elapsed")
    elapsed_time(dt_start_global)
    if row_no % progress_report == 0:
        log_progress(progress_dict, "Progress Report")


if __name__ == "__main__":

    start_time = dt.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    logdir = "logs"
    log_file_path = "./{}/log_{}.log".format(logdir, start_time)

    logging_init(logdir)

    # Need to pre-create HS accounts for all CZOs
    # hs_url = "dev-hs-6.cuahsi.org"
    hs_url = "localhost"

    czo_account_info_dict = {
        "default": {"uname": "czo", "pwd": "123", "hs_url": hs_url},
        # "national": {"uname": "czo_national", "pwd": "123", "hs_url": hs_url},
        # "boulder": {"uname": "czo_boulder", "pwd": "123", "hs_url": hs_url},
    }
    CZO_HS_Account_Obj = CZOHSAccount(czo_account_info_dict)

    # What CZO data to migrate
    PROCESS_FIRST_N_ROWS = 0  # N>0: process the first N rows in file "czo.csv"; N=0:all rows; N<0: a specific list of czo_id see CZO_ID_LIST
    CZO_ID_LIST = [5486]  # a list of czo_id if PROCESS_FIRST_N_ROWS < 0
    READ_CZO_ID_LIST_FROM_CSV = True  # replace CZO_ID_LIST by reading a lsit of czo_id from file "czo_hs_id.csv"
    FIRST_N_ITEM_IN_CSV = 0  # process the first N items in CZO_ID_LIST; 0-all items;
    if READ_CZO_ID_LIST_FROM_CSV and PROCESS_FIRST_N_ROWS == -1:
        CZO_ID_LIST = get_czo_list_from_csv(FIRST_N_ITEM_IN_CSV)
    progress_dict = {"error": [], "success": [], "size_uploaded_mb": 0.0, "big_file_list": []}
    czo_hs_id_lookup_df = pd.DataFrame(columns=["czo_id", "hs_id", "success"])

    dt_start_global = dt.utcnow()
    logging.info("Script started at UTC {}".format(dt_start_global))

    # read csv file into dataframe
    czo_df = pd.read_csv("data/czo.csv")

    # print("Processing rows {} to {}".format(ROW_START, ROW_FINISH))
    # for k, czid in enumerate(range(ROW_FINISH - ROW_START + 1)):
    #     print(ROW_START + k)

    if PROCESS_FIRST_N_ROWS >= 0:

        logging.info("Processing on first {n} rows (0 - all rows)".format(n=PROCESS_FIRST_N_ROWS))
        for index, row in czo_df.iterrows():
            if PROCESS_FIRST_N_ROWS > 0 and index > PROCESS_FIRST_N_ROWS - 1:
                break
            czo_row_dict = row.to_dict()
            migrate_czo_row(czo_row_dict, progress_dict, czo_hs_id_lookup_df, row_no=index + 1)
    else:
        logging.info("Processing on specific {total_rows} czo_ids".format(total_rows=len(CZO_ID_LIST)))
        logging.info(CZO_ID_LIST)
        counter = 0
        for cur_czo_id in CZO_ID_LIST:
            counter += 1
            # process a specific row by czo_id
            df_row = czo_df.loc[czo_df['czo_id'] == cur_czo_id]
            czo_row_dict = czo_res_dict = df_row.to_dict(orient='records')[0]  # convert csv row to dict
            migrate_czo_row(czo_row_dict, progress_dict, czo_hs_id_lookup_df, row_no=counter)

    elapsed_time(dt_start_global)
    log_progress(progress_dict)

    success_error = progress_dict["success"] + progress_dict["error"]

    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.io.json.json_normalize.html
    df_ref_file_list = json_normalize(success_error, "ref_file_list", ["czo_id", "hs_id"], record_prefix="ref_")
    if not df_ref_file_list.empty:
        df_ref_file_list_big_file_filter = df_ref_file_list[
            (df_ref_file_list.ref_big_file_flag == True) & (df_ref_file_list.ref_file_size_mb > 0)]
        # print(df_ref_file_list.to_string())
        logging.info(df_ref_file_list_big_file_filter.to_string())
        logging.info(df_ref_file_list_big_file_filter.sum(axis=0, skipna=True))

    df_concrete_file_list = json_normalize(success_error, "concrete_file_list", ["czo_id", "hs_id"],
                                           record_prefix="concrete_")
    if not df_concrete_file_list.empty:
        df_concrete_file_list_filter = df_concrete_file_list[df_concrete_file_list.concrete_file_size_mb > 0]
        # print(df_concrete_file_list.to_string())
        logging.info(df_concrete_file_list_filter.sum(axis=0, skipna=True))

    counter = 0
    for error_item in progress_dict["error"]:
        counter += 1
        logging.info("{} CZO_ID {} HS_ID {} Error {}".format(counter,
                                                             error_item["czo_id"],
                                                             error_item["hs_id"],
                                                             "|".join([err_msg.replace("\n", " ") for err_msg in
                                                                       error_item["error_msg_list"]])))
    logging.info(czo_hs_id_lookup_df.to_string())
    czo_hs_id_csv_file_path = 'czo_hs_id_{}.csv'.format(start_time)
    czo_hs_id_lookup_df.to_csv(czo_hs_id_csv_file_path, encoding='utf-8', index=False)

    # upload log file and czo_hs_id_csv to hs
    hs = CZO_HS_Account_Obj.get_hs_by_czo("default")
    hs_id = hs.createResource("CompositeResource",
                              "czo2hs migration log files {}".format(start_time),
                              )
    file_id = hs.addResourceFile(hs_id, log_file_path)
    file_id = hs.addResourceFile(hs_id, czo_hs_id_csv_file_path)
    print("Log files uploaded to HS res at {}".format(hs_id))
