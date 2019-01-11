import requests
import tempfile
import os
import validators
import time
import json

def get_spatial_coverage(north_lat, west_long, south_lat, east_long, name=None):
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

    # Hard coded to return "Someone" for now
    hs_creator = {'organization': czos, 'name': "Someone", 'email': "xxxx@czo.org", }
    return hs_creator

# "files": [
#                 {
#                  "file_type": "ReferencedFile",  # ReferencedFile, NetCDF, GeoRaster, GeoFeature
#                  "path_or_url": "https://bcczo.colorado.edu/dataSets/met/entire_B1_TempRH.csv",
#                  "file_name": "entire_B1_TempRH.csv",
#                  "metadata": {"title": "file title",
#                               "keywords": ["file_k1", "file_k2"],
#                               "spatial_coverage": {
#                                                    "type": "point",
#                                                    "units": "Decimal degrees",
#                                                    "east": -99.5447,
#                                                    "north": 38.9574,
#                                                    "projection": "WGS 84 EPSG:4326"
#                                                   },  # "spatial_coverage"
#                              "temporal_coverage": {"start": "2018-02-23",
#                                                     "end": "2018-02-28"
#                                                     },
#                             "extra_metadata": {"file_k1": "file_v1",
#                                                 "file_k2": "file_v2",
#                                             },  # extra_metadata
#
#
#
#                              },  # "metadata"
#                  },  # file 1
#                  {
#                     "file_type": "NetCDF",  # ReferencedFile, NetCDF, GeoRaster, GeoFeature
#                     "path_or_url": r"C:\Users\Drew\PycharmProjects\czo\bulk-resource-creator\sample.nc",
#                     "file_name": "sample.nc",
#                     "metadata": {"title": "file title",
#                                  "keywords": ["file_k1", "file_k2"],
#                                  "spatial_coverage": {
#                                      "type": "point",
#                                      "units": "Decimal degrees",
#                                      "east": -99.5447,
#                                      "north": 38.9574,
#                                      "projection": "WGS 84 EPSG:4326"
#                                  },  # "spatial_coverage"
#                                  "temporal_coverage": {"start": "2018-02-23",
#                                                        "end": "2018-02-28"
#                                                        },
#                                  "extra_metadata": {"file_k1": "file_v1",
#                                                     "file_k2": "file_v2",
#                                                     },  # extra_metadata
#
#                                  },  # "metadata"
#                  },  # file2
#             ]


def _whether_to_harvest_file(filename):

    filename = filename.lower()
    for ext in [".csv", ".doc", ".xls"]:
        if filename.endswith(ext):
            return True
    return False


def get_files(in_str):

    files_list = []
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

            #files_list.append(file_info)
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

            #files_list.append(file_info)
            yield file_info

    #return files_list


def _download_file(url, file_name):
    save_to_base = tempfile.mkdtemp()
    save_to = os.path.join(save_to_base, file_name)
    response = requests.get(url, stream=True)
    with open(save_to, 'wb') as f:
        f.write(response.content)
    return save_to


def get_file_id_by_name(hs, resource_id, fname):

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


