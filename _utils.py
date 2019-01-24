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
                ".xlsx", ".kmz", ".zip", ".xls"]:
        if filename.endswith(ext):
            return True
    return False


def _append_rstr_to_fname(fn, rstrl=6, pre_rstr=None):
    """
    append a small random str to filename: myfile_{RSTR}.txt
    :param fn: original filename
    :param pre_rstr: a string put prior to random string: myfile_{PRE_RSTR}_{RSTR}.txt
    :return: new filename
    """
    if rstrl > 32:
        logging.WARN("Max length of random string is 32 characters")
    rstr = uuid.uuid4().hex[:rstrl]

    if isinstance(pre_rstr, str) and len(pre_rstr) > 0:
        rstr = "{}_{}".format(pre_rstr, rstr)

    file_name_base, file_name_ext = os.path.splitext(fn)
    fn_new = "{}_{}{}".format(file_name_base, rstr, file_name_ext)
    return fn_new


def get_files(in_str):
    """
    This is a generator that returns a resource file dict in each iterate
    :param in_str: file field
    :return: None
    """

    file_name_used_list = []
    for f_str in in_str.split("|"):
        f_info_list = f_str.split("$")
        f_location = f_info_list[0]
        f_topic = f_info_list[1]
        f_url = f_info_list[2]
        f_data_level = f_info_list[3]
        f_private = f_info_list[4]
        f_doi = f_info_list[5]
        f_metadata_url = f_info_list[6]
        if validators.url(f_url):
            f_url_decoded = unquote(f_url)
            file_name = f_url_decoded.split("/")[-1]
            if len(file_name) == 0:
                file_name = f_url_decoded.split("/")[-2]

            file_name = file_name.replace(" ", "_")
            if _whether_to_harvest_file(file_name):
                file_path_local = _download_file(f_url, file_name)
                if file_name in file_name_used_list:
                    file_name = _append_rstr_to_fname(file_name)
                file_info = {"path_or_url": file_path_local,
                             "file_name": file_name,
                             "file_type": "",
                             "metadata": {},
                             }

            else:
                file_name = (f_topic + "-" + f_location).replace(" ", "_")
                if file_name in file_name_used_list:
                    file_name = _append_rstr_to_fname(file_name)
                file_info = {"file_type": "ReferencedFile",
                             "path_or_url": f_url,
                             "file_name": file_name,
                             "metadata": {},
                             }

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

            file_name_used_list.append(file_name)
            yield file_info

        if validators.url(f_metadata_url):
            f_metadata_url_decoded = unquote(f_metadata_url)
            file_name = f_metadata_url_decoded.split("/")[-1]
            if len(file_name) == 0:
                file_name = f_metadata_url_decoded.split("/")[-2]
            file_name = file_name.replace(" ", "_")
            if file_name in file_name_used_list:
                file_name = _append_rstr_to_fname(file_name, pre_rstr="METADATA")
            file_path_local = _download_file(f_metadata_url, file_name)
            file_info = {"path_or_url": file_path_local,
                         "file_name": file_name,
                         "file_type": "",
                         "metadata": {}, }

            file_name_used_list.append(file_name)
            yield file_info


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


def _update_core_metadata(hs_obj, res_id, metadata_dict, message=None):
    """
       Update core metadata for a HydroShare
       :param hs_obj: hs obj initialized by hs_restclient
       :param res_id: resource id
       :param metadata_dict: metadata dict
       :param message: logging message
       :return:
    """
    science_metadata_json = hs_obj.updateScienceMetadata(res_id, metadata=metadata_dict)
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
