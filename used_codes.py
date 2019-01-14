#https://github.com/hydroshare/hydroshare/blob/develop/hs_core/hydroshare/resource.py#L779

# def get_file_id_by_name(hs, resource_id, fname):
#     resource = hs.resource(resource_id)
#     file = ""
#     for f in resource.files.all():
#         file += f.decode('utf8')
#     file_id = -1
#     file_json = json.loads(file)
#     for file in file_json["results"]:
#         if fname.lower() in str(file["url"]):
#             file_id = file["id"]
#     if file_id == -1:
#         print("couldn't find file if for {} in resource {}".format(fname, resource_id))
#     return file_id
#
#
# # read data from csv:
# example_res = {
#     "title": hs_res_title,
#     "resource_type": "CompositeResource",
#     "abstract": hs_res_abstract,
#     "keywords": hs_res_keywords,
#     "sharing_status": "discoverable",
#     "shareable": True,
#     "metadata": [
#         {"creator":
#              hs_creator
#          },  # creator
#
#         {'rights': {'statement': 'This is the rights statement for this CZO resource',
#                     'url': 'http://criticalzone.org/national/'}},
#         # period
#         {'coverage': {'type': 'period', 'value': {  # 'name': 'Name for period coverage',
#             'start': date_start,
#             'end': date_end
#         }
#                       }
#          },
#         # spatial
#         {'coverage': hs_coverage_spatial
#          },
#
#         # # unique value
#         # {'relation': {'type': 'isPartOf', 'value': 'http://hydroshare.org/resource/001'}},
#         # {'relation': {'type': 'isPartOf', 'value': 'http://hydroshare.org/resource/002'}},
#         #
#         # # unique value
#         # {'source': {'derived_from': 'http://hydroshare.org/resource/0001'}},
#         # {'source': {'derived_from': 'http://hydroshare.org/resource/0002'}},
#
#         # fundingagency
#         {'fundingagency': {'agency_name': "NSF",
#                            'award_title': "CZO",
#                            'award_number': "NSF-123-45-6789",
#                            'agency_url': "https://www.nsf.gov",
#                            }
#          },
#     ],  # metadata
#     "extra_metadata": hs_extra_metadata,
#     "files": [
#         {
#             "file_type": "ReferencedFile",  # ReferencedFile, NetCDF, GeoRaster, GeoFeature
#             "path_or_url": "https://bcczo.colorado.edu/dataSets/met/entire_B1_TempRH.csv",
#             "file_name": "entire_B1_TempRH.csv",
#             "metadata": {"title": "file title",
#                          "keywords": ["file_k1", "file_k2"],
#                          "spatial_coverage": {
#                              "type": "point",
#                              "units": "Decimal degrees",
#                              "east": -99.5447,
#                              "north": 38.9574,
#                              "projection": "WGS 84 EPSG:4326"
#                          },  # "spatial_coverage"
#                          "temporal_coverage": {"start": "2018-02-23",
#                                                "end": "2018-02-28"
#                                                },
#                          "extra_metadata": {"file_k1": "file_v1",
#                                             "file_k2": "file_v2",
#                                             },  # extra_metadata
#
#                          },  # "metadata"
#         },  # file 1
#         {
#             "file_type": "NetCDF",  # ReferencedFile, NetCDF, GeoRaster, GeoFeature
#             "path_or_url": r"C:\Users\Drew\PycharmProjects\czo\bulk-resource-creator\sample.nc",
#             "file_name": "sample.nc",
#             "metadata": {"title": "file title",
#                          "keywords": ["file_k1", "file_k2"],
#                          "spatial_coverage": {
#                              "type": "point",
#                              "units": "Decimal degrees",
#                              "east": -99.5447,
#                              "north": 38.9574,
#                              "projection": "WGS 84 EPSG:4326"
#                          },  # "spatial_coverage"
#                          "temporal_coverage": {"start": "2018-02-23",
#                                                "end": "2018-02-28"
#                                                },
#                          "extra_metadata": {"file_k1": "file_v1",
#                                             "file_k2": "file_v2",
#                                             },  # extra_metadata
#
#                          },  # "metadata"
#         },  # file2
#     ]  # files
# }  # example_res


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

# this file holds some used codes as they might be useful down the road



# example_res = {
#     "title": hs_res_title,
#     "resource_type": "CompositeResource",
#     "abstract": hs_res_abstract,
#     "keywords": hs_res_keywords,
#     "sharing_status": "discoverable",
#     "shareable": True,
#     "metadata": [
#                  {"creator":
#                         hs_creator
#                  }, # creator
#                  # {"creator":
#                  #      {'organization': 'ddddd', 'address': 'ggggg', 'name': 'sss', 'email': 'bbb@gmail.com', 'phone': "456"}
#                  # }, # creator
#
#                  {'rights': {'statement': 'This is the rights statement for this CZO resource',
#                              'url': 'http://criticalzone.org/national/'}},
#
#                  {'coverage': {'type': 'period', 'value': {#'name': 'Name for period coverage',
#                                                            'start': date_start,
#                                                            'end': date_end
#                                                            }
#                                }
#                   },
#
#                  {'coverage': hs_coverage_spatial
#                   },
#
#                  # {'coverage': {'type': 'box', 'value': {"units": "Decimal degrees",
#                  #                                     "eastlimit": -104.967635068188,
#                  #                                     "northlimit": 40.3234076878708,
#                  #                                     "southlimit": 39.681928246572,
#                  #                                     "westlimit": -105.738528792788,
#                  #                                     "projection": "WGS 84 EPSG:4326"
#                  #                                    }
#                  #               }
#                  # },
#                  #{'identifier': {'name': 'someIdentifier1234', 'url': "http://some.org/001111"}},
#
#                  # unique value
#                  {'relation': {'type': 'isPartOf', 'value': 'http://hydroshare.org/resource/001'}},
#                  {'relation': {'type': 'isPartOf', 'value': 'http://hydroshare.org/resource/002'}},
#
#                  # unique value
#                  {'source': {'derived_from': 'http://hydroshare.org/resource/0001'}},
#                  {'source': {'derived_from': 'http://hydroshare.org/resource/0002'}},
#
#                  # {'contributor': {'name': 'Kelvin Marshal',
#                  #                  'email': 'kmarshal@yahoo.com',
#                  #                  'organization': 'Utah State University',
#                  #                'homepage': "https://www.google.com/",
#                  #                'identifiers': {'ORCID': 'https://orcid.org/mike_s',
#                  #                                'ResearchGateID': 'https://www.researchgate.net/mike_s'
#                  #                                }
#                  #                  }
#                  #  },  # contributor
#                  # {'contributor': {'name': 'Kelvin Marshal',
#                  #                  'email': 'kmarshal@yahoo.com',
#                  #                  'organization': 'Utah State University',
#                  #                  'homepage': "https://www.google.com/",
#                  #                  'identifiers': {'ORCID': 'https://orcid.org/mike_s',
#                  #                                  'ResearchGateID': 'https://www.researchgate.net/mike_s'
#                  #                                  }
#                  #                  }
#                  #  },  # contributor
#
#                  {'fundingagency': {'agency_name': "NSF",
#                                     'award_title': "Cyber Infrastructure",
#                                     'award_number': "NSF-101-20-6789",
#                                     'agency_url': "https://www.nsf.gov",
#                                     }
#                   }, # fundingagency
#                   # {'fundingagency': {'agency_name': "NSF",
#                   #                   'award_title': "Cyber Infrastructure",
#                   #                   'award_number': "NSF-101-20-6789",
#                   #                   'agency_url': "https://www.nsf.gov",
#                   #                   }
#                   # }, # fundingagency
#
#                 ] , # metadata
#     "extra_metadata": hs_extra_metadata,
#     "files": [
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
#             ]  # files
# }  # example_res
