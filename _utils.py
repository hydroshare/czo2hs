import logging
import tempfile
import os
import json
from datetime import datetime as dt
import uuid
import requests
import validators
from urllib.parse import unquote

requests.packages.urllib3.disable_warnings()

BIG_FILE_SIZE_MB = 100


def get_spatial_coverage(north_lat, west_long, south_lat, east_long, name=None):
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


def get_creator(czos, creator, email):
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

    #res = requests.get(url, stream=True, allow_redirects=True)
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


def _extract_fileinfo_from_url(f_url, file_name_used_dict=None, ref_file_name=None, invalid_url_warning=True):

    if not validators.url(f_url):
        if invalid_url_warning:
            raise Exception("Not a valid URL {}".format(f_url))
        else:
            return None

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


def get_files(in_str):
    """
    This is a generator that returns a resource file dict in each iterate
    :param in_str: file field
    :return: None
    """

    file_name_used_dict = {}
    for f_str in in_str.split("|"):
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
                                               ref_file_name=ref_file_name)
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

        metadata_file_info = _extract_fileinfo_from_url(f_metadata_url,
                                                        file_name_used_dict,
                                                        ref_file_name=ref_file_name + "_metadata",
                                                        invalid_url_warning=False)
        yield metadata_file_info


def _download_file(url, file_name):
    """
       Download a remote czo file to local
       :param url: URL to remote CZ file
       :param save_to: local path to store the file
       :return: None
    """

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


def _update_core_metadata(hs_obj, hs_id, metadata_dict, message=None):
    """
       Update core metadata for a HydroShare
       :param hs_obj: hs obj initialized by hs_restclient
       :param hs_id: resource id
       :param metadata_dict: metadata dict
       :param message: logging message
       :return:
    """
    science_metadata_json = hs_obj.updateScienceMetadata(hs_id, metadata=metadata_dict)
    if not message:
        message = str(metadata_dict)
    logging.info('{message} updated successfully'.format(message=message))
    return science_metadata_json


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
