#https://github.com/hydroshare/hydroshare/blob/develop/hs_core/hydroshare/resource.py#L779
import pandas as pd
czo_df = pd.read_csv("czo.csv")
df_6524 = czo_df.loc[czo_df['czo_id'] == 6524]
dict_6524 = df_6524.to_dict(orient='records')[0]
print dict_6524

czo_res_dict = dict_6524
hs_res_title = czo_res_dict["title"]
hs_res_abstract = "{} \n {} \n {}".format(czo_res_dict["description"],
                                          czo_res_dict["comments"],
                                          czo_res_dict["VARIABLES"],)
hs_res_keywords = []
for item in ("VARIABLES", "TOPICS", "KEYWORDS"):
    hs_res_keywords += czo_res_dict[item].split("|")
hs_res_keywords = set(hs_res_keywords)

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
                            {'organization': 'cuahsi', 'address': 'ajfbsdsj', 'name': 'Jen Morse', 'email': 'BcCZOdata@colorado.edu', },
                 }, # creator
                 # {"creator":
                 #      {'organization': 'ddddd', 'address': 'ggggg', 'name': 'sss', 'email': 'bbb@gmail.com', 'phone': "456"}
                 # }, # creator

                 {'rights': {'statement': 'This is the rights statement for this CZO resource',
                             'url': 'http://criticalzone.org/national/'}},

                 {'coverage': {'type': 'period', 'value': {'name': 'Name for period coverage',
                                                           'start': '7/18/2012',
                                                           'end': '1/1/2015'
                                                           }
                               }
                  },
                 {'coverage': {'type': 'box', 'value': {"units": "Decimal degrees",
                                                     "eastlimit": -104.967635068188,
                                                     "northlimit": 40.3234076878708,
                                                     "southlimit": 39.681928246572,
                                                     "westlimit": -105.738528792788,
                                                     "projection": "WGS 84 EPSG:4326"
                                                    }
                               }
                 },
                 #{'identifier': {'name': 'someIdentifier1234', 'url': "http://some.org/001111"}},

                 # unique value
                 {'relation': {'type': 'isPartOf', 'value': 'http://hydroshare.org/resource/001'}},
                 {'relation': {'type': 'isPartOf', 'value': 'http://hydroshare.org/resource/002'}},

                 # unique value
                 {'source': {'derived_from': 'http://hydroshare.org/resource/0001'}},
                 {'source': {'derived_from': 'http://hydroshare.org/resource/0002'}},

                 # {'contributor': {'name': 'Kelvin Marshal',
                 #                  'email': 'kmarshal@yahoo.com',
                 #                  'organization': 'Utah State University',
                 #                'homepage': "https://www.google.com/",
                 #                'identifiers': {'ORCID': 'https://orcid.org/mike_s',
                 #                                'ResearchGateID': 'https://www.researchgate.net/mike_s'
                 #                                }
                 #                  }
                 #  },  # contributor
                 # {'contributor': {'name': 'Kelvin Marshal',
                 #                  'email': 'kmarshal@yahoo.com',
                 #                  'organization': 'Utah State University',
                 #                  'homepage': "https://www.google.com/",
                 #                  'identifiers': {'ORCID': 'https://orcid.org/mike_s',
                 #                                  'ResearchGateID': 'https://www.researchgate.net/mike_s'
                 #                                  }
                 #                  }
                 #  },  # contributor

                 {'fundingagency': {'agency_name': "NSF",
                                    'award_title': "Cyber Infrastructure",
                                    'award_number': "NSF-101-20-6789",
                                    'agency_url': "https://www.nsf.gov",
                                    }
                  }, # fundingagency
                  # {'fundingagency': {'agency_name': "NSF",
                  #                   'award_title': "Cyber Infrastructure",
                  #                   'award_number': "NSF-101-20-6789",
                  #                   'agency_url': "https://www.nsf.gov",
                  #                   }
                  # }, # fundingagency

                ] , # metadata
    "extra_metadata": {
                        "czo_id": "6524",
                        "subtitle": "ID: B1_TempRH",
                        "CZOS": "Boulder",
                        "FIELD_AREAS": "Boulder Creek Watershed",
                        "location": "B1",
                        "TOPICS": "Air Temperature",
                        "sub_topic": "B1 Air Temp & Relative Humidity (B1_TempRH)",
                        "KEYWORDS": "Air Temperature|Climate|Meteorology|Relative Humidity|B1",
                        "VARIABLES": "date|year|julianday|hour|airtemp_avg|rh_avg",
                        "description": "Climatological data were collected from a ridgetop climate station East of Niwot Ridge (B1 at 2591 m) throughout the year using a Hobo Pro V2 data logger mounted in a Stevenson screen. Parameters measured were air temperature and relative humidity Site History to view logs for complete site history Missing Data to view complete log of missing data. ",
                        "comments": "Data were recorded using a HOBO Onset Pendant starting in 2009, but the site was not maintained or routinely visited until November 2011. At this time, the Stevenson Screen housing the data logger was found knocked over and the sides damaged. All temperature values during this time period are suspect due to increased exposure of logger to wind and sunlight. It is impossible to know when the Stevenson Screen was damaged and thus to know what portion (if any) of these data are usable. The Stevenson Screen was reinstalled on July 18, 2012, when data collection resumed.  Metadata for related data sets are in:https://niwot.colorado.edu/meta_data/b-1mtape.ml.meta.txt https://niwot.colorado.edu/meta_data/b-1preci.ml.meta.txt https://niwot.colorado.edu/meta_data/b-1dp219.ml.meta.txtThe decrease in temperature and increase in humidity beginning September 9th 2013 reflect a large rain/flood event. NOTE: While we strive to produce high-quality accurate data, sometimes errors may occur and data is subject to revision at any time. If you notice an error, please contact the data manager at bcczodata@colorado.edu ",
                        "RELATED_DATASETS": "6522|6523|6521|6526|6524",
                       },  # extra_metadata
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

import json
from hs_restclient import HydroShare, HydroShareAuthBasic

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

auth = HydroShareAuthBasic(username='', password='')
hs = HydroShare(auth=auth, hostname='dev-hs-3.cuahsi.org')
resource_id = hs.createResource(example_res["resource_type"],
                                example_res["title"],
                                keywords=example_res["keywords"],
                                abstract=example_res["abstract"],
                                metadata=json.dumps(example_res["metadata"]),
                                extra_metadata=json.dumps(example_res["extra_metadata"]))
print(resource_id)
for f in example_res["files"]:
    if f["file_type"] == "ReferencedFile":
        hs.createReferencedFile(pid=resource_id,
                                path='data/contents',
                                name=f["file_name"],
                                ref_url=f["path_or_url"])
    else:
        # upload other files
        file_id = hs.addResourceFile(resource_id,
                                     f["path_or_url"])
        # auto file type detection for NetCDF
        # options = {
        #     "file_path": f["file_name"],
        #     "hs_file_type": f["file_type"],
        # }
        # result = hs.resource(resource_id).functions.set_file_type(options)

    # find file id (to be replaced by new hs_restclient)
    file_id = get_file_id_by_name(hs, resource_id, f["file_name"])

    hs.resource(resource_id).files.metadata(file_id, f["metadata"])

# make the resource public
hs.setAccessRules(resource_id, public=True)

# science_metadata_json = hs.getScienceMetadata(resource_id)
# print (json.dumps(science_metadata_json, sort_keys=True, indent=4))


