import logging
import requests
import tempfile
import os
import validators
import json


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

    filename = filename.lower()
    for ext in [".csv", ".doc", ".xls"]:
        if filename.endswith(ext):
            return True
    return False


def get_files(in_str):
    """
    This is a generator that returns a resource file dict in each iterate
    :param in_str: file field
    :return: None
    """

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
            file_name = f_url.split("/")[-1]
            if len(file_name) == 0:
                file_name = f_url.split("/")[-2]

            if _whether_to_harvest_file(file_name):
                file_path_local = _download_file(f_url, file_name)
                file_info = {"path_or_url": file_path_local,
                             "file_name": file_name,
                             "file_type": "",
                             "metadata": {},
                             }

            else:
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

            yield file_info

        if validators.url(f_metadata_url):
            file_name = f_metadata_url.split("/")[-1]
            if len(file_name) == 0:
                file_name = f_metadata_url.split("/")[-2]
            file_path_local = _download_file(f_metadata_url, file_name)
            file_info = {"path_or_url": file_path_local,
                         "file_name": file_name,
                         "file_type": "",
                         "metadata": {}, }
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
        print("couldn't find file for {} in resource {}".format(fname, resource_id))
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
