import logging
import shutil
import os
import json
from datetime import datetime as dt

import pandas as pd
from pandas.io.json import json_normalize
from hs_restclient import HydroShare, HydroShareAuthBasic

from _utils import get_spatial_coverage, get_creator, \
    get_files, get_file_id_by_name, _update_core_metadata, \
    elapsed_time, prepare_logging_str

script_start_dt = dt.utcnow()
script_start_dt_str = script_start_dt.strftime("%Y-%m-%d_%H-%M-%S")
log_file_path = "./log_{0}.log".format(script_start_dt_str)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler()
    ])


def _get_hs_obj():
    """
    init hs_restclient obj using global vars
    :return: hs_obj
    """

    auth = HydroShareAuthBasic(username=hs_user_name, password=hs_user_pwd)
    if "hydroshare.org" in hs_host_url or "cuahsi.org" in hs_host_url:
        hs = HydroShare(auth=auth, hostname=hs_host_url)
    else:
        hs = HydroShare(auth=auth, hostname=hs_host_url, port=8000, use_https=False, verify=False)
    return hs


def _create_hs_res_from_czo(czo_res_dict, index=-99):
    """
    Create a HydroShare resource from a CZO data row
    :param czo_res_dict: dict of CZO data row
    :return: {"success": False,
                 "czo_id": -1,
                 "hs_id": -1,
                 "ref_file_list": [],
                 "concrete_file_list": [],
                 "error_msg": "success",
                 }
    """
    record_dict = {"success": False,
                 "czo_id": -1,
                 "hs_id": -1,
                 "ref_file_list": [],
                 "concrete_file_list": [],
                 "error_msg": "success",
                 }
    try:

        czo_id = czo_res_dict["czo_id"]
        record_dict["czo_id"] = czo_id
        logging.info("Working on Row {index} CZO_ID {czo_id}".format(index=index, czo_id=czo_id))

        # parse file info
        czo_files = czo_res_dict['COMPONENT_FILES-location$topic$url$data_level$private$doi$metadata_url']

        # CZO Name
        czos = czo_res_dict["CZOS"]
        # hs title
        hs_res_title = czos + ":" + czo_res_dict["title"]

        # hs abstract
        hs_res_abstract = "{czos} \n\n" \
                          "{subtitle} \n\n" \
                          "[Description]\n {description} \n\n" \
                          "[Comments]\n {comments} \n\n" \
                          "[Variables]\n {VARIABLES} \n\n".format(czos=czos,
                                                                  subtitle=czo_res_dict["subtitle"],
                                                                  description=czo_res_dict["description"],
                                                                  comments=czo_res_dict["comments"],
                                                                  VARIABLES=czo_res_dict["VARIABLES"], )
        # if date_range_comments exists, append to hs abstract
        date_range_comments = czo_res_dict["date_range_comments"]
        if isinstance(date_range_comments, str) and len(date_range_comments) > 0:
            hs_res_abstract = hs_res_abstract + "[Date Range Comments] \n {date_range_comments}\n\n" \
                .format(date_range_comments=date_range_comments)
        # hs abstract end

        # hs keywords
        hs_res_keywords = []
        for item in ("VARIABLES", "TOPICS", "KEYWORDS", "CZOS"):
            hs_res_keywords += czo_res_dict[item].split("|")
        hs_res_keywords = map(str.lower, hs_res_keywords)
        hs_res_keywords = set(hs_res_keywords)
        if "" in hs_res_keywords:
            hs_res_keywords.remove("")
        # hs keywords end

        # hs creator/author
        contact_email = czo_res_dict["contact"]
        creator_name = czo_res_dict["creator"]
        hs_creator = get_creator(czos, creator_name, contact_email)
        # hs creator/author end

        # hs coverage
        # temporal
        date_start = czo_res_dict["date_start"]
        date_end = czo_res_dict["date_end"]
        # spatial
        east_long = czo_res_dict["east_long"]
        west_long = czo_res_dict["west_long"]
        south_lat = czo_res_dict["south_lat"]
        north_lat = czo_res_dict["north_lat"]
        field_areas = czo_res_dict["FIELD_AREAS"]
        location = czo_res_dict["location"]
        hs_coverage_spatial = get_spatial_coverage(north_lat, west_long, south_lat, east_long,
                                                   name=field_areas + "-" + location)
        hs_coverage_period = {'type': 'period', 'value': {'start': date_start, 'end': date_end, }}
        # hs coverage end

        # hs res level extended metadata
        hs_extra_metadata = dict(
            (str(name), str(czo_res_dict[name])) for name in ['czo_id', 'subtitle', 'CZOS', 'FIELD_AREAS',
                                                              'location', 'TOPICS', 'sub_topic', 'KEYWORDS',
                                                              'VARIABLES', 'description', 'comments', 'RELATED_DATASETS',
                                                              'date_range_comments', ])

        hs = _get_hs_obj()

        # Since current HydroShare REST API and hs_restclient DO NOT return specific error message,
        # sending a Big JSON to create a complete HydroShare resource is hard to debug
        # which part is wrong if error arises.
        # The workaround is updating metadata one at a time to isolate potential errors

        # create a Composite Resource with title, extra metadata
        # extra metadata is uploaded here because I haven't found a way to update it separately
        hs_id = hs.createResource("CompositeResource",
                                        hs_res_title,
                                        extra_metadata=json.dumps(hs_extra_metadata)
                                        )
        czo_hs_id = {"czo_id": czo_id, "hs_id": hs_id, "success": "T"}
        record_dict["hs_id"] = hs_id
        logging.info('HS resource created at: {hs_id}'.format(hs_id=hs_id))

        # update Abstract/Description
        science_metadata_json = _update_core_metadata(hs, hs_id,
                                                      {"description": hs_res_abstract},
                                                      message="Abstract")

        # update Keywords/Subjects
        science_metadata_json = _update_core_metadata(hs, hs_id,
                                                      {"subjects": [{"value": kw} for kw in hs_res_keywords]},
                                                      message="Keyword")

        # update creators
        science_metadata_json = _update_core_metadata(hs, hs_id,
                                                      {"creators": [hs_creator]},
                                                      message="Author")

        # update coverage
        # spatial coverage and period coverage must be updated at the same time as updating any single one would remove the other
        science_metadata_json = _update_core_metadata(hs, hs_id,
                                                      {'coverages': [hs_coverage_spatial, hs_coverage_period]},
                                                      message="Coverage")

        # metadata still not working!!!! https://github.com/hydroshare/hs_restclient/issues/97
        # rights, funding_agencies, extra_metadata

        for f in get_files(czo_files):
            if f is None:
                continue
            try:
                logging.info("Creating file: {}".format(str(f)))
                if f["file_type"] == "ReferencedFile":
                    # resp_dict = hs.createReferencedFile(pid=hs_id,
                    #                                     path='data/contents',
                    #                                     name=f["file_name"],
                    #                                     ref_url=f["path_or_url"])
                    # file_id = resp_dict["file_id"]

                    # log ref file
                    record_dict["ref_file_list"].append(f)

                else:
                    # upload other files with auto file type detection
                    # file_id = hs.addResourceFile(hs_id,
                    #                              f["path_or_url"])
                    tmpfile_folder_path = os.path.dirname(f["path_or_url"])
                    try:
                        shutil.rmtree(tmpfile_folder_path)
                    except:
                        pass
                    # find file id (to be replaced by new hs_restclient)
                    file_id = get_file_id_by_name(hs, hs_id, f["file_name"])

                    # log concrete file
                    record_dict["concrete_file_list"].append(f)

                hs.resource(hs_id).files.metadata(file_id, f["metadata"])
            except Exception as ex_file:
                logging.error(ex_file)

        # make the resource public
        try:
            hs.setAccessRules(hs_id, public=True)
            logging.info("Resource is made Public")
        except Exception:
            logging.error("Failed to make Resource Public")
            pass

        # science_metadata_json = hs.getScienceMetadata(hs_id)
        # print (json.dumps(science_metadata_json, sort_keys=True, indent=4))

        logging.info("Done with Row {index} CZO_ID: {czo_id}".format(index=index, czo_id=czo_id))
        _success = True

    except Exception as ex:
        _success = False
        logging.error("!" * 10 + "Error" + "!" * 10)

        ex_type = "type: " + str(type(ex))
        ex_doc = prepare_logging_str(ex, "__doc__")
        ex_msg = prepare_logging_str(ex, "message")
        ex_str = prepare_logging_str(ex, "__str__")
        record_dict["error_msg"] = ex_type + ex_doc + ex_msg + ex_str

        logging.error(ex_type + ex_doc + ex_msg + ex_str)
        logging.error(ex)

    finally:
        record_dict["success"] = _success
        return record_dict


def _log_progress(progress_dict, header="Summary"):
    """
    Write progress report to screen and logging file
    :param progress_dict: a dict contains progress info
    :param header: the header to print
    :return: None
    """

    error_counter = len(progress_dict["error"])
    success_counter = len(progress_dict["success"])
    logging.info("*" * 10 + "{}".format(header) + "*" * 10)
    logging.info(
        "Total: {}; Success: {}; Error {}".format(error_counter + success_counter, success_counter, error_counter))


def _log_uploaded_file_stats(record_dict):

    concrete_file_num = len(record_dict["concrete_file_list"])
    concrete_file_size_total = sum([f["file_size_mb"] if f["file_size_mb"]>0 else 0 for f in record_dict["concrete_file_list"]])
    logging.info("Uploaded concrete files: {}; Size {} MB".format(concrete_file_num, concrete_file_size_total))

    logging.info("Created ref files: {}".format(len(record_dict["ref_file_list"])))
    ref_big_file_list = list(filter(lambda f: (f["big_file_flag"] == True), record_dict["ref_file_list"]))
    logging.info("Big ref files list:".format(len(ref_big_file_list)))
    for f_big in ref_big_file_list:
        logging.info(f_big)


def _create_hs_resource_from_czo_row_dict(czo_row_dict, row_no=1, progress_report=5):
    """
    Create a HS resource from a CZO row dict
    :param czo_row_dict: czo data row dict
    :param row_no: row number
    :param progress_report: print out progress report every N rows
    :return: None
    """
    global progress_dict
    global czo_hs_id_lookup_df

    logging.info("-" * 40)
    dt_start_resource = dt.utcnow()
    logging.info("Start migrating one Resource at UTC {}".format(dt_start_resource))

    mgr_result = _create_hs_res_from_czo(czo_row_dict, index=row_no)

    if mgr_result["success"]:
        progress_dict["success"].append(mgr_result)
    else:
        progress_dict["error"].append(mgr_result)

    # append czo_id vs hs_id to lookup table
    czo_hs_id_lookup_dict = {"czo_id": mgr_result["czo_id"],
                             "hs_id": mgr_result["hs_id"],
                             "success": mgr_result["success"]

    }

    czo_hs_id_lookup_df = czo_hs_id_lookup_df.append(czo_hs_id_lookup_dict, ignore_index=True)
    _log_uploaded_file_stats(mgr_result)

    elapsed_time(dt_start_resource, prompt_str="Resource Creation Time Elapsed")
    elapsed_time(dt_start_global)
    if row_no % progress_report == 0:
        _log_progress(progress_dict, "Progress Report")


def _czo_list_from_csv(_num):
    """
    Read czo ids from a csv file
    :return: a list of czo id
    """
    czo_list = []
    df = pd.read_csv("data/czo_hs_id.csv")
    row_dict_list = df.loc[df['success'] == "F"].to_dict(orient='records')
    counter = 0
    for item in row_dict_list:
        if _num > 0 and counter >= _num:
            break
        czo_list.append(item["czo_id"])
        counter += 1

    return czo_list


# TODO user friendly error when credentials are wrong or server not reachable
# Global Vars

# Which HydroShare to talk to
hs_host_url = "dev-hs-6.cuahsi.org"
hs_user_name = "drew"
hs_user_pwd = "123456"

# hs_host_url = "127.0.0.1"
# hs_user_name = "drew"
# hs_user_pwd = "123"

# hs_host_url = "www.hydroshare.org"
# hs_user_name = ""
# hs_user_pwd = ""

PROCESS_FIRST_N_ROWS = -1  # N>0: process the first N rows; N=0:all rows; N<0: a specific row
CZO_ID_LIST = [2666]  # 2407 a list of czo_id if PROCESS_FIRST_N_ROWS = -1
READ_CZO_ID_LIST_FROM_CSV = True
FIRST_N_ITEM_IN_CSV = 10


if READ_CZO_ID_LIST_FROM_CSV and PROCESS_FIRST_N_ROWS == -1:
    CZO_ID_LIST = _czo_list_from_csv(FIRST_N_ITEM_IN_CSV)
progress_dict = {"error": [], "success": [], "size_uploaded_mb": 0.0, "big_file_list": []}
czo_hs_id_lookup_df = pd.DataFrame(columns=["czo_id", "hs_id", "success"])


if __name__ == "__main__":

    dt_start_global = dt.utcnow()
    logging.info("Script started at UTC {}".format(dt_start_global))
    logging.info("Connecting to {} with account {}".format(hs_host_url, hs_user_name))

    # read csv file into dataframe
    czo_df = pd.read_csv("data/czo.csv")

    if PROCESS_FIRST_N_ROWS >= 0:

        logging.info("Processing first {n} rows (0 - all rows)".format(n=PROCESS_FIRST_N_ROWS))
        # loop through dataframe rows
        for index, row in czo_df.iterrows():
            if PROCESS_FIRST_N_ROWS > 0 and index > PROCESS_FIRST_N_ROWS - 1:
                break
            czo_row_dict = row.to_dict()  # convert csv row to dict
            _create_hs_resource_from_czo_row_dict(czo_row_dict, index+1)
    else:

        logging.info("Processing {total_rows} czo_ids".format(total_rows=len(CZO_ID_LIST)))
        logging.info(CZO_ID_LIST)
        counter = 0
        for cur_czo_id in CZO_ID_LIST:
            counter += 1
            # process a specific row by czo_id
            df_row = czo_df.loc[czo_df['czo_id'] == cur_czo_id]
            czo_row_dict = czo_res_dict = df_row.to_dict(orient='records')[0]  # convert csv row to dict
            _create_hs_resource_from_czo_row_dict(czo_row_dict, counter)

    elapsed_time(dt_start_global)
    _log_progress(progress_dict)

    success_error = progress_dict["success"] + progress_dict["error"]

    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.io.json.json_normalize.html
    df_ref_file_list = json_normalize(success_error, "ref_file_list", ["czo_id", "hs_id"], record_prefix="ref_")
    df_ref_file_list_big_file_filter = df_ref_file_list[(df_ref_file_list.ref_big_file_flag == True) & (df_ref_file_list.ref_file_size_mb > 0)]
    # print(df_ref_file_list.to_string())
    print(df_ref_file_list_big_file_filter.to_string())
    print(df_ref_file_list_big_file_filter.sum(axis=0, skipna=True))

    df_concrete_file_list = json_normalize(success_error, "concrete_file_list", ["czo_id", "hs_id"], record_prefix="concrete_")
    df_concrete_file_list_filter = df_concrete_file_list[df_concrete_file_list.concrete_file_size_mb > 0]
    # print(df_concrete_file_list.to_string())
    print(df_concrete_file_list_filter.sum(axis=0, skipna=True))

    counter = 0
    for error_item in progress_dict["error"]:
        counter += 1
        logging.info("{} CZO_ID {} HS_ID {} Error {}".format(counter,
                                                              error_item["czo_id"],
                                                              error_item["hs_id"],
                                                              error_item["error_msg"].replace("\n", " ")))
    logging.info(czo_hs_id_lookup_df.to_string())
    czo_hs_id_csv_file_path = 'czo_hs_id_{}.csv'.format(script_start_dt_str)
    czo_hs_id_lookup_df.to_csv(czo_hs_id_csv_file_path, encoding='utf-8', index=False)

    # upload log file and czo_hs_id_csv to hs
    hs = _get_hs_obj()
    hs_id = hs.createResource("CompositeResource",
                                    "czo2hs migration log files {}".format(script_start_dt_str),
                                    )
    file_id = hs.addResourceFile(hs_id, log_file_path)
    file_id = hs.addResourceFile(hs_id, czo_hs_id_csv_file_path)
    print("Log files uploaded to HS res at {}".format(hs_id))
