import json
import logging
import os
import shutil
import tempfile
import uuid
from datetime import datetime as dt
from urllib.parse import unquote

import pandas as pd
import requests
import validators
from hs_restclient import HydroShare, HydroShareAuthBasic

from settings import BIG_FILE_SIZE_MB

requests.packages.urllib3.disable_warnings()


def _get_spatial_coverage(north_lat, west_long, south_lat, east_long, name=None):
    """
    Assemble HydroShare spatial coverage metadata dict
    :param north_lat: north limit of the bounding box
    :param west_long: west limit of the bounding box
    :param south_lat: south limit of the bounding box
    :param east_long: east limit of the bounding box
    :param name: name of the spatial coverage
    :return: a dict of spatial coverage metadata
    """

    if east_long == west_long and south_lat == north_lat:
        # point
        hs_coverage_spatial = {'type': 'point', 'value': {"units": "Decimal degrees",
                                                          "north": north_lat,
                                                          "east": east_long,
                                                          "projection": "WGS 84 EPSG:4326"
                                                          }
                               }
    else:  # box
        hs_coverage_spatial = {'type': 'box', 'value': {"units": "Decimal degrees",
                                                        "eastlimit": east_long,
                                                        "northlimit": north_lat,
                                                        "southlimit": south_lat,
                                                        "westlimit": west_long,
                                                        "projection": "WGS 84 EPSG:4326"
                                                        }
                               }
    if name and len(name) > 0:
        hs_coverage_spatial["value"]["name"] = name

    return hs_coverage_spatial


def _get_creator(czos, creator, email):
    """
    Assemble HydroShare Creator metadata dict
    :param czos: czos name
    :param creator: creator field
    :param email: creator email
    :return: HydroShare Creator metadata dict
    """

    # Hard coded to return "Someone" for now
    hs_creator = {'organization': czos, 'name': "Someone", 'email': "xxxx@czo.org", }
    return hs_creator


def _whether_to_harvest_file(filename):
    """
    check file extension and decide whether to harvest/download
    :param filename: filename
    :return: True: harvest/download;
    """

    filename = filename.lower()
    for ext in [".hdr", ".docx", ".csv", ".txt", ".pdf",
                ".xlsx", ".kmz", ".zip", ".xls", ".7z", ".kmz"]:
        if filename.endswith(ext):
            return True
    return False


def _is_big_file(f_size_mb):
    if f_size_mb > BIG_FILE_SIZE_MB:
        return True
    return False


def _check_file_size_MB(url):
    # res = requests.get(url, stream=True, allow_redirects=True)
    res = requests.head(url, allow_redirects=True)
    f_size_str = res.headers.get('content-length')
    if f_size_str is None:
        logging.warning("Cannot detect file size in HTTP header {}".format(url))
        return -999
    f_size_byte = int(f_size_str)
    f_size_mb = f_size_byte / 1024.0 / 1024.0
    return f_size_mb


def _append_rstr_to_fname(fn, split_ext=True, rstrl=6, pre_rstr=None):
    """
    append a small random str to filename: myfile_{RSTR}.txt
    :param fn: original filename
    :param split_ext: True - insert string before ext; False: append str to end
    :param pre_rstr: a string put prior to random string: myfile_{PRE_RSTR}_{RSTR}.txt
    :return: new filename
    """
    if rstrl > 32:
        logging.warning("Max length of random string is 32 characters")
    rstr = uuid.uuid4().hex[:rstrl]

    if isinstance(pre_rstr, str) and len(pre_rstr) > 0:
        rstr = "{}_{}".format(pre_rstr, rstr)

    if split_ext:
        file_name_base, file_name_ext = os.path.splitext(fn)
        fn_new = "{}_{}{}".format(file_name_base, rstr, file_name_ext)
    else:
        fn_new = "{}_{}".format(fn, rstr)
    return fn_new


def _append_suffix_str_to_fname(fn, suffix_str, split_ext=True):
    """
    append a small random str to filename: myfile_{RSTR}.txt
    :param fn: original filename
    :param split_ext: True - insert string before ext; False: append str to end
    :param suffix_str: suffix string
    :return: new filename
    """
    suffix_str = str(suffix_str)
    if split_ext:
        file_name_base, file_name_ext = os.path.splitext(fn)
        fn_new = "{}_{}{}".format(file_name_base, suffix_str, file_name_ext)
    else:
        fn_new = "{}_{}".format(fn, suffix_str)
    return fn_new


def _handle_duplicated_file_name(file_name, file_name_used_dict, split_ext=True):
    file_name_new = file_name
    if file_name in file_name_used_dict:
        file_suffix_int = file_name_used_dict[file_name]
        file_suffix_int_new = file_suffix_int + 1
        file_name_new = _append_suffix_str_to_fname(file_name, file_suffix_int_new, split_ext=split_ext)
        file_name_used_dict[file_name] = file_suffix_int_new
    else:
        file_name_used_dict[file_name] = 0

    return file_name_new


def _extract_fileinfo_from_url(f_url, file_name_used_dict=None, ref_file_name=None, invalid_url_warning=False):
    file_info = None

    if not validators.url(f_url):
        if invalid_url_warning:
            raise Exception("Not a valid URL {}".format(f_url))
        else:
            return file_info

    if file_name_used_dict is None:
        file_name_used_dict = {}

    if ref_file_name is None or len(ref_file_name) == 0:
        ref_file_name = "ref_file_{}".format(uuid.uuid4().hex[:6])

    f_url_decoded = unquote(f_url)
    file_name = f_url_decoded.split("/")[-1]
    if len(file_name) == 0:
        file_name = f_url_decoded.split("/")[-2]

    file_name = file_name.replace(" ", "_")

    harvestable_file_flag = _whether_to_harvest_file(file_name)
    big_file_flag = False
    file_size_mb = -1
    if harvestable_file_flag:
        file_size_mb = _check_file_size_MB(f_url)
        big_file_flag = _is_big_file(file_size_mb)
        if big_file_flag:
            logging.warning("{} MB big file detected at {}".format(int(file_size_mb), f_url))

    if harvestable_file_flag and not big_file_flag:

        file_name = _handle_duplicated_file_name(file_name, file_name_used_dict)

        file_path_local = _download_file(f_url, file_name)
        file_info = {"file_type": "",
                     "path_or_url": file_path_local,
                     "file_name": file_name,
                     "big_file_flag": False,
                     }

    else:  # Referenced File Type
        if not harvestable_file_flag:
            # url doesnt explicitly point to a file name
            file_name = ref_file_name.replace(" ", "_").replace(",", "_").replace("/", "_").replace("\\", "_")
            file_name = _handle_duplicated_file_name(file_name, file_name_used_dict, split_ext=False)
        else:
            # for harvestable but too big file
            file_name = _handle_duplicated_file_name(file_name, file_name_used_dict)
            big_file_flag = True

        file_info = {"file_type": "ReferencedFile",
                     "path_or_url": f_url,
                     "file_name": file_name,
                     "big_file_flag": big_file_flag,
                     }

    file_info["file_size_mb"] = file_size_mb
    file_info["original_url"] = f_url
    file_info["metadata"] = {}

    return file_info


def _get_files(in_str, record_dict=None):
    """
    This is a generator that returns a resource file dict in each iterate
    :param in_str: file field
    :return: None
    """

    file_name_used_dict = {}
    for f_str in in_str.split("|"):
        try:
            f_info_list = f_str.split("$")
            f_location = f_info_list[0]
            f_topic = f_info_list[1]
            f_url = f_info_list[2]
            f_data_level = f_info_list[3]
            f_private = f_info_list[4]
            f_doi = f_info_list[5]
            f_metadata_url = f_info_list[6]

            ref_file_name = "REF_" + f_topic + "-" + f_location
            file_info = _extract_fileinfo_from_url(f_url,
                                                   file_name_used_dict,
                                                   ref_file_name=ref_file_name,
                                                   invalid_url_warning=True)

            file_info["metadata"] = {"title": f_topic,

                                     # "spatial_coverage": {
                                     #                      "name": f_location,
                                     #                     },  # "spatial_coverage"
                                     "extra_metadata": {"private": f_private,
                                                        "data_level": f_data_level,
                                                        "metadata_url": f_metadata_url,
                                                        "url": f_url,
                                                        "location": f_location,
                                                        "doi": f_doi,
                                                        },  # extra_metadata
                                     }
            yield file_info
        except Exception as ex:
            extra_msg = "Failed to parse resource file from component {}".format(f_str)
            _log_exception(ex, record_dict=record_dict, extra_msg=extra_msg)
            yield 1

        try:
            metadata_file_info = _extract_fileinfo_from_url(f_metadata_url,
                                                            file_name_used_dict,
                                                            ref_file_name=ref_file_name + "_metadata",
                                                            invalid_url_warning=False)
            if metadata_file_info is None:
                yield 2

            yield metadata_file_info
        except Exception as ex:
            extra_msg = "Failed to parse metadata file from component {}".format(f_str)
            _log_exception(ex, record_dict=record_dict, extra_msg=extra_msg)
            yield 1


def _download_file(url, file_name):
    """
       Download a remote czo file to local
       :param url: URL to remote CZ file
       :param save_to: local path to store the file
       :return: None
    """
    # TODO try catch and log
    # TODO handle for rate limiting
    save_to_base = tempfile.mkdtemp()
    save_to = os.path.join(save_to_base, file_name)
    response = requests.get(url, stream=True)
    with open(save_to, 'wb') as f:
        f.write(response.content)
    return save_to


def get_file_id_by_name(hs, resource_id, fname):
    """
    This is a temporary workaround as current hs_restclient doens return id of newly created file;
    Loop through all files in a resource and return id of a file by filename;
    :param hs: hs obj from initialized hs_restclient
    :param resource_id: res id
    :param fname: the filename to search
    :return: file id
    """

    resource = hs.resource(resource_id)
    file = ""
    for f in resource.files.all():
        file += f.decode('utf8')
    file_id = -1
    file_json = json.loads(file)
    for file in file_json["results"]:
        if fname.lower() in str(file["url"]).lower():
            file_id = file["id"]
    if file_id == -1:
        logging.error("Couldn't find file for {} in resource {}".format(fname, resource_id))
    return file_id


def _update_core_metadata(hs_obj, hs_id, metadata_dict, message=None, record_dict=None):
    """
       Update core metadata for a HydroShare
       :param hs_obj: hs obj initialized by hs_restclient
       :param hs_id: resource id
       :param metadata_dict: metadata dict
       :param message: logging message
       :return:
    """
    result = True
    science_metadata_json = None
    try:
        science_metadata_json = hs_obj.updateScienceMetadata(hs_id, metadata=metadata_dict)
        if not message:
            message = str(metadata_dict)
        logging.info('{message} updated successfully'.format(message=message))
        result = science_metadata_json
    except Exception as ex:
        extra_msg = 'Failed to update {message}: {metadata}'.format(message=message, metadata=metadata_dict)
        result = False
        _log_exception(ex, record_dict=record_dict, extra_msg=extra_msg)
    finally:
        return result, science_metadata_json


def elapsed_time(dt_start, return_type="log", prompt_str="Total Time Elapsed"):
    dt_utcnow = dt.utcnow()
    dt_timedelta = dt_utcnow - dt_start
    if return_type == "log":
        logging.info("{0}: {1}".format(prompt_str, dt_timedelta))
    elif return_type == "str":
        return str(dt_timedelta)
    else:
        return dt_timedelta


def prepare_logging_str(ex, attr, one_line=True):
    logging_str = attr + ": " + str(getattr(ex, attr, "NO " + attr))
    if one_line:
        logging_str = logging_str.replace("\r\n", " ")
        logging_str = logging_str.replace("\n", " ")
    return logging_str


def _log_exception(ex, record_dict=None, extra_msg=""):
    logging.error("!" * 10 + "Error" + "!" * 10)

    ex_type = "type: " + str(type(ex))
    ex_doc = prepare_logging_str(ex, "__doc__")
    ex_msg = prepare_logging_str(ex, "message")
    ex_str = prepare_logging_str(ex, "__str__")
    if record_dict is not None:
        record_dict["error_msg_list"].append(extra_msg + ex_type + ex_doc + ex_msg + ex_str)

    logging.error(extra_msg)
    logging.error(ex_type + ex_doc + ex_msg + ex_str)
    logging.error(ex)
    logging.error("!" * 25)


def log_progress(progress_dict, header="Summary"):
    """
    Write progress report to screen and logging file
    :param progress_dict: a dict contains progress info
    :param header: the header to print
    :return: None
    """

    error_counter = len(progress_dict["error"])
    success_counter = len(progress_dict["success"])
    logging.info("*" * 10 + "{}".format(header) + "*" * 10)
    logging.info("Total: {}; Success: {}; Error {}".format(error_counter + success_counter,
                                                           success_counter,
                                                           error_counter))


def log_uploaded_file_stats(record_dict):
    concrete_file_num = len(record_dict["concrete_file_list"])
    concrete_file_size_total = sum(
        [f["file_size_mb"] if f["file_size_mb"] > 0 else 0 for f in record_dict["concrete_file_list"]])
    logging.info("Uploaded concrete files: {}; Size {} MB".format(concrete_file_num, concrete_file_size_total))

    logging.info("Created ref files: {}".format(len(record_dict["ref_file_list"])))
    ref_big_file_list = list(filter(lambda f: (f["big_file_flag"] == True), record_dict["ref_file_list"]))
    logging.info("Big ref files list:".format(len(ref_big_file_list)))
    for f_big in ref_big_file_list:
        logging.info(f_big)


def get_czo_list_from_csv(_num):
    """
    Read czo ids from a csv file
    :return: a list of czo id
    """
    czo_list = []
    df = pd.read_csv("data/czo_hs_id.csv")
    row_dict_list = df.loc[df['success'] == False].to_dict(orient='records')
    counter = 0
    for item in row_dict_list:
        if _num > 0 and counter >= _num:
            break
        czo_list.append(item["czo_id"])
        counter += 1

    return czo_list


def _get_hs_obj(hs_user_name, hs_user_pwd, hs_host_url):
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


def create_hs_res_from_czo_row(czo_res_dict, czo_hs_account_obj, index=-99, ):
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
                   "error_msg_list": [],
                   }

    _success = False
    try:
        czo_id = czo_res_dict["czo_id"]
        record_dict["czo_id"] = czo_id
        logging.info("Working on Row {index} CZO_ID {czo_id}".format(index=index, czo_id=czo_id))

        # parse file info
        czo_files = czo_res_dict['COMPONENT_FILES-location$topic$url$data_level$private$doi$metadata_url']

        # CZO Name
        czos = czo_res_dict["CZOS"]
        czo_primary = czos.split('|')[0]
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
        hs_creator = _get_creator(czos, creator_name, contact_email)
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
        hs_coverage_spatial = _get_spatial_coverage(north_lat, west_long, south_lat, east_long,
                                                    name=field_areas + "-" + location)
        hs_coverage_period = {'type': 'period', 'value': {'start': date_start, 'end': date_end, }}
        # hs coverage end

        # hs res level extended metadata
        hs_extra_metadata = dict(
            (str(name), str(czo_res_dict[name])) for name in ['czo_id', 'subtitle', 'CZOS', 'FIELD_AREAS',
                                                              'location', 'TOPICS', 'sub_topic', 'KEYWORDS',
                                                              'VARIABLES', 'description', 'comments',
                                                              'RELATED_DATASETS',
                                                              'date_range_comments', ])
        hs = czo_hs_account_obj.get_hs_by_czo(czo_primary)

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
        record_dict["hs_id"] = hs_id
        logging.info('HS resource created at: {hs_id}'.format(hs_id=hs_id))

        # update Abstract/Description
        _success_abstract, _ = _update_core_metadata(hs, hs_id,
                                                     {"description": hs_res_abstract},
                                                     message="Abstract",
                                                     record_dict=record_dict)

        # update Keywords/Subjects
        _success_keyword, _ = _update_core_metadata(hs, hs_id,
                                                    {"subjects": [{"value": kw} for kw in hs_res_keywords]},
                                                    message="Keyword",
                                                    record_dict=record_dict)

        # update creators
        _success_creator, _ = _update_core_metadata(hs, hs_id,
                                                    {"creators": [hs_creator]},
                                                    message="Author",
                                                    record_dict=record_dict)

        # update coverage
        # spatial coverage and period coverage must be updated at the same time as updating any single one would remove the other
        _success_coverage, _ = _update_core_metadata(hs, hs_id,
                                                     {'coverages': [hs_coverage_spatial, hs_coverage_period]},
                                                     message="Coverage",
                                                     record_dict=record_dict)

        # metadata still not working!!!! https://github.com/hydroshare/hs_restclient/issues/97
        # rights, funding_agencies, extra_metadata

        _success_file = True
        for f in _get_files(czo_files, record_dict=record_dict):
            if f == 1:
                _success_file = False
                continue
            elif f == 2:
                continue
            elif f is None:
                continue
            try:
                logging.info("Creating file: {}".format(str(f)))
                if f["file_type"] == "ReferencedFile":
                    resp_dict = hs.createReferencedFile(pid=hs_id,
                                                        path='data/contents',
                                                        name=f["file_name"],
                                                        ref_url=f["path_or_url"])
                    file_id = resp_dict["file_id"]

                    # log ref file
                    record_dict["ref_file_list"].append(f)

                else:
                    # upload other files with auto file type detection
                    file_id = hs.addResourceFile(hs_id,
                                                 f["path_or_url"])
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
                _success_file = False
                extra_msg = "Failed upload file to HS {}: ".format(json.dumps(f))
                _log_exception(ex_file, record_dict=record_dict, extra_msg=extra_msg)

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
        if _success_abstract and _success_keyword and \
                _success_coverage and _success_creator and _success_file:
            _success = True

    except Exception as ex:
        _success = False
        extra_msg = "Failed to migrate CZO dict {}: ".format(json.dumps(czo_res_dict))
        _log_exception(ex, record_dict=record_dict, extra_msg=extra_msg)

    finally:
        record_dict["success"] = _success
        return record_dict
