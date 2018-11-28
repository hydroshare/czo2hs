import nltk
# nltk.download('punkt')
# nltk.download('averaged_perceptron_tagger')
# nltk.download('maxent_ne_chunker')
# nltk.download('words')
import requests
import tempfile
import os
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

    hs_creator = {'organization': czos, 'name': "Someone", 'email': "xxxx@czo.org", }
    #hs_creator = {'organization': czos, 'name': creator, 'email': email, }
    # hs_creator = {'organization': 'ddddd', 'address': 'ggggg', 'name': 'sss', 'email': 'bbb@gmail.com', 'phone': "456"}
    #print (get_human_names(creator))
    # if ";" in creator: # multiple creator separated by ;
    #     pass
    # elif "," in creator:
    #
    # pass
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
import validators
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
            path_or_url = f_url
            file_type = "ReferencedFile"
            metadata= {"title": f_topic,

                          # "spatial_coverage": {
                          #                      "name": f_location,
                          #                     },  # "spatial_coverage"
                        "extra_metadata": {"private": f_private,
                                           "data_level": f_data_level,
                                           "metadata_url": f_metadata_url,
                                           "url": f_url,
                                           "location": f_location
                                        },  # extra_metadata
                         }

            file = {"file_type": file_type, "path_or_url": path_or_url, "file_name": file_name,
                "metadata": metadata}
            files_list.append(file)

        if validators.url(f_metadata_url):
            file_name = f_metadata_url.split("/")[-1]
            if len(file_name) > 0:
                save_to_base = tempfile.mkdtemp()
                save_to = os.path.join(save_to_base, file_name)
                _download_file(f_metadata_url, save_to)
                path_or_url = save_to
                file = {"path_or_url": path_or_url, "file_name": file_name, "file_type": "", "metadata": {},
                        }
                files_list.append(file)


    return files_list

def _download_file(url, save_to):
    response = requests.get(url, stream=True)
    with open(save_to, 'wb') as f:
        f.write(response.content)


# # from nameparser.parser import HumanName
#
# def get_human_names(text):
#     tokens = nltk.tokenize.word_tokenize(text)
#     pos = nltk.pos_tag(tokens)
#     sentt = nltk.ne_chunk(pos, binary = False)
#     person_list = []
#     person = []
#     name = ""
#     for subtree in sentt.subtrees(filter=lambda t: t.node == 'PERSON'):
#         for leaf in subtree.leaves():
#             person.append(leaf[0])
#         if len(person) > 1: #avoid grabbing lone surnames
#             for part in person:
#                 name += part + ' '
#             if name[:-1] not in person_list:
#                 person_list.append(name[:-1])
#             name = ''
#         person = []
#
#     return (person_list)
#
# # import nltk
# # from nameparser.parser import HumanName
# # from nltk.corpus import wordnet
#
#
# def get_human_names(text):
#     person_list = []
#     tokens = nltk.tokenize.word_tokenize(text)
#     pos = nltk.pos_tag(tokens)
#     sentt = nltk.ne_chunk(pos, binary = False)
#
#     person = []
#     name = ""
#     for subtree in sentt.subtrees(filter=lambda t: t.label() == 'PERSON'):
#         for leaf in subtree.leaves():
#             person.append(leaf[0])
#         if len(person) > 1: #avoid grabbing lone surnames
#             for part in person:
#                 name += part + ' '
#             if name[:-1] not in person_list:
#                 person_list.append(name[:-1])
#             name = ''
#         person = []
#     return person_list
