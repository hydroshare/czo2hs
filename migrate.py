import logging
import os
import time

import pandas as pd
from pandas.io.json import json_normalize

from accounts import CZOHSAccount
from api_helpers import create_hs_res_from_czo_row
from settings import LOG_DIR, CZO_ACCOUNTS, CLEAR_LOGS, \
    CZO_DATA_CSV, CZO_ID_LIST_TO_MIGRATE, START_ROW_INDEX, END_ROW_INDEX, \
    RUN_2ND_PASS
from utils_logging import text_emphasis, elapsed_time, log_uploaded_file_stats
from second_pass import second_pass


def logging_init(log_prefix="log"):
    """
    Configure environment and logging settings
    :return: string relative log dir and name
    """
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
    elif CLEAR_LOGS:
        for _file in os.listdir(LOG_DIR):
            file_path = os.path.join(LOG_DIR, _file)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
            except Exception as e:
                print(e)
    # Show hour time of day then use time.time() to ensure newest is always at bottom in folder
    timestamp_suffix = start_time.strftime("%Y-%m-%d_%Hh-%Mm_{}".format(int(time.time())))
    log_file_name = "{}_{}.log".format(log_prefix, timestamp_suffix)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)-5.5s]  %(message)s",
        handlers=[
            logging.FileHandler(os.path.join(LOG_DIR, log_file_name)),
            logging.StreamHandler()
        ])
    return os.path.join(LOG_DIR, log_file_name), timestamp_suffix


def migrate_czo_row(czo_row_dict, czo_accounts, row_no=1):
    """
    Create a HS resource from a CZO row dict
    :param czo_row_dict:
    :param czo_accounts:
    :param row_no:
    :return:
    """
    global error_status
    _start = time.time()
    # logging.info(text_emphasis("", char='=', num_char=40))

    full_data_item = create_hs_res_from_czo_row(czo_row_dict, czo_accounts, index=row_no)

    if full_data_item["success"]:
        error_status["success"].append(full_data_item)
    else:
        error_status["error"].append(full_data_item)

    czo_hs_id_lookup_dict = {"czo_id": full_data_item["czo_id"],
                             "hs_id": full_data_item["hs_id"],
                             "success": full_data_item["success"],
                             "uname": full_data_item["uname"],
                             "elapsed_time": time.time() - _start,
                             "public": full_data_item["public"],
                             "maps": "|".join(full_data_item["maps"]),
                             }

    log_uploaded_file_stats(full_data_item)
    logging.info("{} - Success: {} - Error {}".format(elapsed_time(_start, time.time()),
                                                      len(error_status["success"]), len(error_status["error"])))
    return czo_hs_id_lookup_dict


def output_status(success_error, czo_accounts):
    """
    Parse and log the status
    :param: success_error:
    :param: czo_accounts:
    :return:
    """
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.io.json.json_normalize.html
    df_bad_ref_file_list = json_normalize(success_error, "bad_ref_file_list", ["czo_id", "hs_id"],
                                          record_prefix="bad_ref_")
    if (not df_bad_ref_file_list.empty) and df_bad_ref_file_list.shape[0] > 0:
        logging.info(text_emphasis("Summary on Not-resolving Ref Files"))
        logging.info(df_bad_ref_file_list.to_string())

    df_ref_file_list = json_normalize(success_error, "ref_file_list", ["czo_id", "hs_id", "uname"], record_prefix="ref_")
    if (not df_ref_file_list.empty) and df_ref_file_list.shape[0] > 0:
        logging.info(text_emphasis("Summary on Big Ref Files"))
        df_ref_file_list_big_file_filter = df_ref_file_list[
            (df_ref_file_list.ref_big_file_flag == True) & (df_ref_file_list.ref_file_size_mb > 0)]

        logging.info(df_ref_file_list_big_file_filter.to_string(columns=["ref_file_name", "ref_file_size_mb", "ref_original_url", "czo_id", "hs_id", 'uname']))
        logging.info(df_ref_file_list_big_file_filter.sum(axis=0, skipna=True))

    df_concrete_file_list = json_normalize(success_error, "concrete_file_list", ["czo_id", "hs_id"],
                                           record_prefix="concrete_")
    if (not df_concrete_file_list.empty) and df_concrete_file_list.shape[0] > 0:
        logging.info(text_emphasis("Summary on Migrated Concrete Files"))

        df_concrete_file_list_filter = df_concrete_file_list[df_concrete_file_list.concrete_file_size_mb > 0]
        logging.info(df_concrete_file_list_filter.sum(axis=0, skipna=True))

    for k, error_item in enumerate(error_status["error"]):
        errors = "|".join([err_msg.replace("\n", " ") for err_msg in error_item["error_msg_list"]])
        logging.info("{} CZO_ID {} HS_ID {} Error {}".format(k + 1, error_item["czo_id"], error_item["hs_id"], errors))
    return czo_accounts.get_hs_by_czo("default")


def main():
    log_file_path, timestamp_suffix = logging_init()
    logging.info("Migration Start {}".format(start_time.asctime()))

    czo_accounts = CZOHSAccount(CZO_ACCOUNTS)
    czo_hs_id_lookup_df = pd.DataFrame(columns=["success", "czo_id", "hs_id", "uname", "elapsed_time",
                                                "public", "maps"]).\
        astype(dtype={"elapsed_time": "timedelta64[s]", })

    czo_data = pd.read_csv(CZO_DATA_CSV)
    czo_data = czo_data[czo_data.czo_id > 1]
    czo_data.czo_id = czo_data.czo_id.astype(int)
    czo_id_list = CZO_ID_LIST_TO_MIGRATE.copy()
    if czo_id_list is None or len(czo_id_list) == 0:
        end_index = END_ROW_INDEX
        if end_index > czo_data.shape[0] - 1:
            end_index = czo_data.shape[0] - 1
            logging.warning("end_index reset to {}".format(end_index))

        indices = range(START_ROW_INDEX, end_index+1)
        czo_id_list = czo_data.iloc[indices]["czo_id"].tolist()
    logging.info("Processing on {} czo_ids: {}".format(len(czo_id_list), czo_id_list))

    for i in range(len(czo_id_list)):
        czo_id = czo_id_list[i]
        # process a specific row by czo_id
        czo_row_dict = czo_data.loc[czo_data['czo_id'] == czo_id].to_dict(orient='records')[0]
        result = migrate_czo_row(czo_row_dict, czo_accounts, row_no=i + 1)
        czo_hs_id_lookup_df = czo_hs_id_lookup_df.append(result, ignore_index=True)
        if i % 5 == 0:
            print(czo_hs_id_lookup_df)

    success_error = error_status["success"] + error_status["error"]

    logging.info(czo_hs_id_lookup_df.to_string())

    results_file = os.path.join(LOG_DIR, 'lookup_{}.csv'.format(timestamp_suffix))
    logging.info("Saving Lookup Table to {}".format(results_file))
    czo_hs_id_lookup_df["elapsed_time"] = czo_hs_id_lookup_df["elapsed_time"].\
        apply(lambda sec: "{:.0f} sec | {:.2f} min".format(sec, sec / 60))
    czo_hs_id_lookup_df.to_csv(results_file, encoding='utf-8', index=False)

    if RUN_2ND_PASS:
        second_pass(CZO_DATA_CSV, results_file, czo_accounts)

    # upload logs and results to HS
    hs = output_status(success_error, czo_accounts)

    # existing_hs_ids = [x for x in hs.resources()]
    # scimeta = [hs.getScienceMetadata(x.get('resource_id')) for x in existing_hs_ids]
    # print(scimeta)

    hs_id = hs.createResource("CompositeResource",
                              "czo2hs migration log files {}".format(timestamp_suffix),)
    hs.addResourceFile(hs_id, log_file_path)
    hs.addResourceFile(hs_id, results_file)

    logging.info("Migration log files uploaded to HydroShare with ID {}".format(hs_id))


if __name__ == "__main__":
    start_time = time
    start = time.time()
    error_status = {"error": [], "success": [], "size_uploaded_mb": 0.0, "big_file_list": []}

    try:
        main()
    except KeyboardInterrupt:
        print("\nExit ok")
    finally:
        logging.info("Total Migration {}".format(elapsed_time(start, time.time())))
