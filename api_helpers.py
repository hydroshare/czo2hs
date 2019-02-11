import json
import logging
import os
import shutil

import pandas as pd
import requests
from hs_restclient import HydroShare, HydroShareAuthBasic

from file_ops import extract_fileinfo_from_url, retry_func
from settings import logger, USE_PREDOWNLOAD
from utils_logging import log_exception

# TODO move to settings and test
requests.packages.urllib3.disable_warnings()


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


def get_files(in_str, record_dict=None):
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
            file_info = extract_fileinfo_from_url(f_url,
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
            log_exception(ex, record_dict=record_dict, extra_msg=extra_msg)
            yield 1

        try:
            metadata_file_info = extract_fileinfo_from_url(f_metadata_url,
                                                           file_name_used_dict,
                                                           ref_file_name=ref_file_name + "_metadata",
                                                           invalid_url_warning=False)
            if metadata_file_info is None:
                yield 2

            yield metadata_file_info
        except Exception as ex:
            extra_msg = "Failed to parse metadata file from component {}".format(f_str)
            log_exception(ex, record_dict=record_dict, extra_msg=extra_msg)
            yield 1


def safe_get(url, timeout=10, headers={}, stream=False, verify=True):
    """
    Attempts to retrieve resource at url
    :param url: url
    :param timeout: timeout
    :return:
    """
    r = {"url_asked": url, "status_code": 400, "error": "", "text": "", "history": ""}
    try:
        req = requests.get(url, headers=headers, timeout=timeout, stream=stream, verify=verify)
        r['requested'] = url
        r['status_code'] = req.status_code
        r['history'] = str(req.history)
        r['text'] = req.text
        r['url'] = req.url
        r['content'] = req.content
    except requests.exceptions.ConnectTimeout as e:
        logger.debug("ConnectTimeout %s", url)  # probing for links manually generates lots of errors
        r['error'] = str(e)
    except requests.packages.urllib3.exceptions.ConnectTimeoutError as e:
        logger.debug("ConnectTimeoutError %s", url)
        r['error'] = str(e)
    except requests.packages.urllib3.exceptions.MaxRetryError as e:
        logger.debug("MaxRetryError %s", url)
        r['error'] = str(e)
    except requests.packages.urllib3.exceptions.ReadTimeoutError as e:
        logger.debug("ReadTimeoutError %s")
        r['error'] = str(e)
    except ConnectionError as e:
        logger.debug("ConnectionError %s", url)
        r['error'] = str(e)
    except requests.exceptions.SSLError as e:
        logger.debug("SSLError %s", url)
        r['error'] = str(e)
    except requests.exceptions.ConnectionError as e:
        logger.debug("ConnectError %s", url)
        r['error'] = str(e)
    except requests.exceptions.ReadTimeout as e:
        logger.debug("ReadTimeout %s", url)
        r['error'] = str(e)
    except requests.exceptions.TooManyRedirects as e:
        logger.debug("TooManyRedirects %s", url)
        r['error'] = e
    except Exception as e:
        logger.debug("Unhandled exception %s", url)
        r['error'] = str(e)
    finally:
        return r


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
        log_exception(ex, record_dict=record_dict, extra_msg=extra_msg)
    finally:
        return result, science_metadata_json


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


def _extract_value_from_df_row_dict(row_dict, key, required=True):
    v = str(row_dict.get(key))
    if len(v) > 0 and v.lower() not in ["nan", "na", "n/a", r"n\a", "none"]:
        return v
    if required:
        raise Exception("Failed to parse key {} from {}".format(key, str(row_dict)))
    return None


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


def create_hs_res_from_czo_row(czo_res_dict, czo_hs_account_obj, index=-99, ):
    """
    TODO break this function up into more functions for readability and modularity
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
                   "primary_owner": None,
                   }

    _success = False
    try:
        # parse resource-level metadata
        # parse czo_id
        czo_id = _extract_value_from_df_row_dict(czo_res_dict, "czo_id")
        record_dict["czo_id"] = czo_id
        logging.info("Working on NO.{index} CZO_ID {czo_id}".format(index=index, czo_id=czo_id))

        # parse CZOS
        czos = _extract_value_from_df_row_dict(czo_res_dict, "CZOS")
        czos_list = czos.split('|')
        czo_primary = czos_list[0]
        if len(czos_list) > 1:
            czo_primary = "national"  # cross-czo res goes to national account
            logging.info("Cross-CZO resource to be created by National account: {}".format(czos_list))

        # parse title, subtitle, description, comments
        title = _extract_value_from_df_row_dict(czo_res_dict, "title")
        subtitle = _extract_value_from_df_row_dict(czo_res_dict, "subtitle", required=False)
        description = _extract_value_from_df_row_dict(czo_res_dict, "description")
        comments = _extract_value_from_df_row_dict(czo_res_dict, "comments", required=False)

        # parse FIELD_AREAS, location
        field_areas = _extract_value_from_df_row_dict(czo_res_dict, "FIELD_AREAS")
        field_areas_list = field_areas.split('|')
        location = _extract_value_from_df_row_dict(czo_res_dict, "location")

        # parse VARIABLES
        variables = _extract_value_from_df_row_dict(czo_res_dict, "VARIABLES")
        variables_list = variables.split('|')

        # parse date_start, date_end, date_range_comments
        date_start = _extract_value_from_df_row_dict(czo_res_dict, "date_start")
        date_end = _extract_value_from_df_row_dict(czo_res_dict, "date_end", required=False)
        date_range_comments = _extract_value_from_df_row_dict(czo_res_dict, "date_range_comments", required=False)

        # parse bouding box
        east_long = _extract_value_from_df_row_dict(czo_res_dict, "east_long")
        west_long = _extract_value_from_df_row_dict(czo_res_dict, "west_long")
        south_lat = _extract_value_from_df_row_dict(czo_res_dict, "south_lat")
        north_lat = _extract_value_from_df_row_dict(czo_res_dict, "north_lat")

        # parse file info
        czo_files = czo_res_dict['COMPONENT_FILES-location$topic$url$data_level$private$doi$metadata_url']

        # parse citation, data_doi
        citation = _extract_value_from_df_row_dict(czo_res_dict, "citation", required=False)
        dataset_doi = _extract_value_from_df_row_dict(czo_res_dict, "dataset_doi", required=False)

        # parse TOPICS, KEYWORDS
        topics = _extract_value_from_df_row_dict(czo_res_dict, "TOPICS")
        topics_list = topics.split('|')
        keywords = _extract_value_from_df_row_dict(czo_res_dict, "KEYWORDS", required=False)
        keywords_list = keywords.split('|') if keywords is not None else []

        # parse DISCIPLINES
        disciplines = _extract_value_from_df_row_dict(czo_res_dict, "DISCIPLINES", required=False)
        disciplines_list = disciplines.split('|') if disciplines is not None else []

        # parse sub_topic
        sub_topic = _extract_value_from_df_row_dict(czo_res_dict, "sub_topic", required=False)

        # parse EXTERNAL_LINKS-url$link_text,PUBLICATIONS_OF_THIS_DATA, RELATED_DATASETS
        external_links = _extract_value_from_df_row_dict(czo_res_dict, "EXTERNAL_LINKS-url$link_text", required=False)
        publications_of_this_data = _extract_value_from_df_row_dict(czo_res_dict, "PUBLICATIONS_OF_THIS_DATA",
                                                                    required=False)
        publications_using_this_data = _extract_value_from_df_row_dict(czo_res_dict, "PUBLICATIONS_USING_THIS_DATA",
                                                                       required=False)
        related_datasets = _extract_value_from_df_row_dict(czo_res_dict, "RELATED_DATASETS", required=False)
        related_datasets_list = related_datasets.split('|') if related_datasets is not None else []

        # end parse resource-level metadata

        # hs title
        hs_res_title = title

        # hs abstract
        hs_res_abstract = "{title}".format(title=title)
        if subtitle is not None:
            hs_res_abstract += "\n\n{subtitle}".format(subtitle=subtitle)
        hs_res_abstract += "\n\nCZO: {czos_list}".format(czos_list=", ".join(czos_list))
        hs_res_abstract += "\n\nField Area: {field_areas_list}".format(field_areas_list=", ".join(field_areas_list))
        hs_res_abstract += "\n\nLocation: {location}".format(location=location)
        hs_res_abstract += "\n\nStart Date: {date_start}".format(date_start=date_start)
        hs_res_abstract += "\nEnd Date: {date_end}".format(date_end=date_end if date_end is not None else "")
        if date_range_comments is not None:
            hs_res_abstract += "\nDate Range Comments: {date_range_comments}".format(
                date_range_comments=date_range_comments)
        hs_res_abstract += "\n\nDescription: {description}".format(description=description)
        if citation is not None:
            hs_res_abstract += "\n\nCitation: {citation}".format(citation=citation)
        if dataset_doi is not None:
            hs_res_abstract += "\n\nDataset DOI: {dataset_doi}".format(dataset_doi=dataset_doi)
        # hs abstract end

        # hs keywords - czos, FIELD_AREAS, TOPICS, VARIABLES, Keyword?????
        hs_res_keywords = [] + czos_list + field_areas_list + topics_list + keywords_list
        hs_res_keywords = map(str.lower, hs_res_keywords)
        hs_res_keywords = set(hs_res_keywords)
        if "" in hs_res_keywords:
            hs_res_keywords.remove("")
        # hs keywords end

        # hs creator/author
        # hard coded
        hs_creator = _get_creator(czo_primary, "", "")
        # hs creator/author end

        # hs coverage
        coverage_name = ", ".join([", ".join(field_areas_list), location])
        hs_coverage_spatial = _get_spatial_coverage(north_lat, west_long, south_lat, east_long,
                                                    name=coverage_name)
        hs_coverage_period = {'type': 'period',
                              'value': {'start': date_start, 'end': date_end if date_end is not None else "1/1/2099", }}
        # hs coverage end

        # hs res level extended metadata
        hs_extra_metadata = dict(czo_id=czo_id,
                                 czos=", ".join(czos_list),
                                 field_areas=", ".join(field_areas_list),
                                 location=location,
                                 topics=", ".join(topics_list),
                                 description=description,
                                 variables=", ".join(variables_list),
                                 )
        if subtitle is not None:
            hs_extra_metadata["subtitle"] = subtitle
        if disciplines is not None:
            hs_extra_metadata["disciplines"] = ", ".join(disciplines_list)
        if sub_topic is not None:
            hs_extra_metadata["sub_topic"] = sub_topic
        if date_range_comments is not None:
            hs_extra_metadata["date_range_comments"] = date_range_comments
        if keywords is not None:
            hs_extra_metadata["keywords"] = ", ".join(keywords_list)
        if citation is not None:
            hs_extra_metadata["citation"] = citation
        if comments is not None:
            hs_extra_metadata["comments"] = comments
        if external_links is not None:
            hs_extra_metadata["external_links"] = external_links
        if publications_of_this_data is not None:
            hs_extra_metadata["publications_of_this_data"] = publications_of_this_data
        if publications_using_this_data is not None:
            hs_extra_metadata["publications_using_this_data"] = publications_using_this_data
        if related_datasets is not None:
            hs_extra_metadata["related_datasets"] = ", ".join(related_datasets_list)

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
        record_dict["primary_owner"] = hs.auth.username  # export owner of this hs res
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
        for f in get_files(czo_files, record_dict=record_dict):
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
                    # resp_dict = hs.createReferencedFile(pid=hs_id,
                    #                                     path='data/contents',
                    #                                     name=f["file_name"],
                    #                                     ref_url=f["path_or_url"])
                    kw = {"pid": hs_id, "path": "data/contents", "name": f['file_name'], "ref_url": f['path_or_url']}
                    resp_dict = retry_func(hs.createReferencedFile, kwargs=kw)
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
                    except Exception:
                        pass
                    # find file id (to be replaced by new hs_restclient)
                    file_id = get_file_id_by_name(hs, hs_id, f["file_name"])

                    # log concrete file
                    record_dict["concrete_file_list"].append(f)

                hs.resource(hs_id).files.metadata(file_id, f["metadata"])
            except Exception as ex_file:
                _success_file = False
                extra_msg = "Failed upload file to HS {}: ".format(json.dumps(f))
                log_exception(ex_file, record_dict=record_dict, extra_msg=extra_msg)

        # make the resource public
        try:
            hs.setAccessRules(hs_id, public=True)
            logging.info("Resource is made Public")
        except Exception:
            logging.error("Failed to make Resource Public")

        # science_metadata_json = hs.getScienceMetadata(hs_id)
        # print (json.dumps(science_metadata_json, sort_keys=True, indent=4))

        logging.info("Done with NO.{index} CZO_ID: {czo_id}".format(index=index, czo_id=czo_id))
        if _success_abstract and _success_keyword and \
                _success_coverage and _success_creator and _success_file:
            _success = True

    except Exception as ex:
        _success = False
        extra_msg = "Failed to migrate CZO dict {}: ".format(json.dumps(czo_res_dict))
        log_exception(ex, record_dict=record_dict, extra_msg=extra_msg)

    finally:
        record_dict["success"] = _success
        return record_dict
