#https://github.com/hydroshare/hydroshare/blob/develop/hs_core/hydroshare/resource.py#L779
import pandas as pd
import json
from hs_restclient import HydroShare, HydroShareAuthBasic
from czo2hs_parser import get_spatial_coverage, get_creator, get_files
from _czo import _update_core_metadata

#hs_host_url = "dev-hs-6.cuahsi.org"
hs_host_url = "www.hydroshare.org"
hs_user_name = ""
hs_user_pwd = ""

hs_host_url = "127.0.0.1"
hs_user_name = ""
hs_user_pwd = ""

czo_df = pd.read_csv("czo.csv")
df_6524 = czo_df.loc[czo_df['czo_id'] == 2414]

for index, row in czo_df.iterrows():
    try:
        if index > 2:
            break
        df_6524 = row
        pass
        #dict_6524 = df_6524.to_dict(orient='records')[0]

        dict_6524 = df_6524.to_dict()
        print (dict_6524)


        czo_res_dict = dict_6524
        czo_files = czo_res_dict['COMPONENT_FILES-location$topic$url$data_level$private$doi$metadata_url']
        get_files(czo_files)

        czos = czo_res_dict["CZOS"]
        # title
        hs_res_title = czo_res_dict["title"]
        # abstract
        hs_res_abstract = "{} \n\n " \
                        "[Description]\n {} \n\n " \
                          "[Comments]\n {} \n\n " \
                          "[Variables]\n {} \n\n".format(czo_res_dict["subtitle"],
                                                  czo_res_dict["description"],
                                                  czo_res_dict["comments"],
                                                  czo_res_dict["VARIABLES"],)
        date_range_comments = czo_res_dict["date_range_comments"]
        if isinstance(date_range_comments, str) and len(date_range_comments) > 0 :
            hs_res_abstract = hs_res_abstract + "[Date Range Comments] \n {}\n\n".format(date_range_comments)

        # keywords
        hs_res_keywords = []
        for item in ("VARIABLES", "TOPICS", "KEYWORDS", "CZOS"):
            hs_res_keywords += czo_res_dict[item].split("|")
        hs_res_keywords = map(str.lower, hs_res_keywords)
        hs_res_keywords = set(hs_res_keywords)
        if "" in hs_res_keywords:
            hs_res_keywords.remove("")

        czo_id = czo_res_dict["czo_id"]
        contact_email = czo_res_dict["contact"]
        creator_name = czo_res_dict["creator"]

        hs_creator = get_creator(czos, creator_name, contact_email)

        # coverage
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
        hs_coverage_spatial = get_spatial_coverage(north_lat, west_long, south_lat, east_long, name=field_areas + "-" + location)
        hs_coverage_period = {'type': 'period', 'value': {#'name': 'Name for period coverage',
                                                                   'start': date_start,
                                                                   'end': date_end,
                                                                   }
                              }
        hs_extra_metadata = dict((str(name), str(czo_res_dict[name])) for name in ['czo_id', 'subtitle', 'CZOS', 'FIELD_AREAS',
                                                                 'location', 'TOPICS', 'sub_topic', 'KEYWORDS',
                                                                 'VARIABLES', 'description', 'comments', 'RELATED_DATASETS',
                                                                                   'date_range_comments',])

        # read data from csv:
        example_res = {
            "title": hs_res_title,
            "resource_type": "CompositeResource",
            "abstract": hs_res_abstract,
            "keywords": hs_res_keywords,
            "sharing_status": "discoverable",
            "shareable": True,
            "metadata": [
                         {"creator":
                                hs_creator
                         }, # creator

                         {'rights': {'statement': 'This is the rights statement for this CZO resource',
                                     'url': 'http://criticalzone.org/national/'}},
                         # period
                         {'coverage': {'type': 'period', 'value': {#'name': 'Name for period coverage',
                                                                   'start': date_start,
                                                                   'end': date_end
                                                                   }
                                       }
                          },
                         # spatial
                         {'coverage': hs_coverage_spatial
                          },

                         # # unique value
                         # {'relation': {'type': 'isPartOf', 'value': 'http://hydroshare.org/resource/001'}},
                         # {'relation': {'type': 'isPartOf', 'value': 'http://hydroshare.org/resource/002'}},
                         #
                         # # unique value
                         # {'source': {'derived_from': 'http://hydroshare.org/resource/0001'}},
                         # {'source': {'derived_from': 'http://hydroshare.org/resource/0002'}},

                         # fundingagency
                         {'fundingagency': {'agency_name': "NSF",
                                            'award_title': "CZO",
                                            'award_number': "NSF-123-45-6789",
                                            'agency_url': "https://www.nsf.gov",
                                            }
                          },
                        ], # metadata
            "extra_metadata": hs_extra_metadata,
            "files": [
                        {
                         "file_type": "ReferencedFile",  # ReferencedFile, NetCDF, GeoRaster, GeoFeature
                         "path_or_url": "https://bcczo.colorado.edu/dataSets/met/entire_B1_TempRH.csv",
                         "file_name": "entire_B1_TempRH.csv",
                         "metadata": {"title": "file title",
                                      "keywords": ["file_k1", "file_k2"],
                                      "spatial_coverage": {
                                                           "type": "point",
                                                           "units": "Decimal degrees",
                                                           "east": -99.5447,
                                                           "north": 38.9574,
                                                           "projection": "WGS 84 EPSG:4326"
                                                          },  # "spatial_coverage"
                                     "temporal_coverage": {"start": "2018-02-23",
                                                            "end": "2018-02-28"
                                                            },
                                    "extra_metadata": {"file_k1": "file_v1",
                                                        "file_k2": "file_v2",
                                                    },  # extra_metadata



                                     },  # "metadata"
                         },  # file 1
                         {
                            "file_type": "NetCDF",  # ReferencedFile, NetCDF, GeoRaster, GeoFeature
                            "path_or_url": r"C:\Users\Drew\PycharmProjects\czo\bulk-resource-creator\sample.nc",
                            "file_name": "sample.nc",
                            "metadata": {"title": "file title",
                                         "keywords": ["file_k1", "file_k2"],
                                         "spatial_coverage": {
                                             "type": "point",
                                             "units": "Decimal degrees",
                                             "east": -99.5447,
                                             "north": 38.9574,
                                             "projection": "WGS 84 EPSG:4326"
                                         },  # "spatial_coverage"
                                         "temporal_coverage": {"start": "2018-02-23",
                                                               "end": "2018-02-28"
                                                               },
                                         "extra_metadata": {"file_k1": "file_v1",
                                                            "file_k2": "file_v2",
                                                            },  # extra_metadata

                                         },  # "metadata"
                         },  # file2
                    ]  # files
        }  # example_res


        def get_file_id_by_name(hs, resource_id, fname):
            resource = hs.resource(resource_id)
            file = ""
            for f in resource.files.all():
                file += f.decode('utf8')
            file_id = -1
            file_json = json.loads(file)
            for file in file_json["results"]:
                if fname.lower() in str(file["url"]):
                    file_id = file["id"]
            if file_id == -1:
                print("couldn't find file if for {} in resource {}".format(fname, resource_id))
            return file_id

        auth = HydroShareAuthBasic(username=hs_user_name, password=hs_user_pwd)
        if "hydroshare.org" in hs_host_url or "cuahsi.org" in hs_host_url:
            hs = HydroShare(auth=auth, hostname=hs_host_url)
        else:
            hs = HydroShare(auth=auth, hostname=hs_host_url, port=8000, use_https=False, verify=False)

        # Step 1: create an empty resource with Resource Type and Title
        resource_id = hs.createResource(example_res["resource_type"],
                                        example_res["title"],
                                        keywords=example_res["keywords"],
                                        abstract=example_res["abstract"],
                                        )

        # # https://hs-restclient.readthedocs.io/en/latest/
        # metadata = {
        #     "title": "A new title for my resource",
        #     "coverages":
        #         [ {"type": "period", "value": {"start": "01/01/2000", "end": "12/12/2010"}}
        #          ],
        #     'source': {'derived_from': 'http://hydroshare.org/resource/0001'},
        #     "creators": [
        #         {"name": "John Smith", "organization": "USU"},
        #         {"name": "Lisa Miller", "email": "lisa_miller@gmail.com"}
        #     ]
        # }
        # science_metadata_json = _update_core_metadata(hs, resource_id, metadata)


        # core metadata
        # creators
        science_metadata_json = _update_core_metadata(hs, resource_id, {"creators": [hs_creator]})

        # spatial coverage
        science_metadata_json = _update_core_metadata(hs, resource_id, {'coverages': [{"type": "period", "value": {"start": "01/01/2000", "end": "12/12/2010"}}]})

        # period coverage
        science_metadata_json = _update_core_metadata(hs, resource_id, {'coverages': [hs_coverage_spatial]})


        # rights
        right= {'statement': 'This is the rights statement for this CZO resource',
                    'url': 'http://criticalzone.org/national/'}
        science_metadata_json = _update_core_metadata(hs, resource_id, {'rights': [right]})

        #
        # resource_id = hs.createResource(example_res["resource_type"],
        #                                 example_res["title"],
        #                                 keywords=example_res["keywords"],
        #                                 abstract=example_res["abstract"],
        #                                 metadata=json.dumps(example_res["metadata"]),
        #                                 extra_metadata=json.dumps(example_res["extra_metadata"]),
        #                                 )

        print(resource_id)
        #for f in example_res["files"]:
        for f in get_files(czo_files):

            file_id = None
            if f["file_type"] == "ReferencedFile":
                resp_dict = hs.createReferencedFile(pid=resource_id,
                                        path='data/contents',
                                        name=f["file_name"],
                                        ref_url=f["path_or_url"])
                file_id = resp_dict["file_id"]
            else:
                # upload other files
                # auto file type detect
                file_id = hs.addResourceFile(resource_id,
                                             f["path_or_url"])


            # # find file id (to be replaced by new hs_restclient)
            # if not file_id:
            #     file_id = get_file_id_by_name(hs, resource_id, f["file_name"])

            hs.resource(resource_id).files.metadata(file_id, f["metadata"])

        # make the resource public
        hs.setAccessRules(resource_id, public=True)

        # science_metadata_json = hs.getScienceMetadata(resource_id)
        # print (json.dumps(science_metadata_json, sort_keys=True, indent=4))

    except Exception as e:
        pass
print("Done")