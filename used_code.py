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
         },  # creator

        {'rights': {'statement': 'This is the rights statement for this CZO resource',
                    'url': 'http://criticalzone.org/national/'}},
        # period
        {'coverage': {'type': 'period', 'value': {  # 'name': 'Name for period coverage',
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
    ],  # metadata
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