import json
import logging
import os
import shutil
import tempfile

import pandas as pd
import requests
from hs_restclient import HydroShare, HydroShareAuthBasic

from file_ops import extract_fileinfo_from_url, retry_func
from settings import logger, headers
from utils_logging import log_exception

# TODO move to settings and test
requests.packages.urllib3.disable_warnings()
pd.set_option('display.max_colwidth', 200)


def get_creator_hs_metadata(creator_list):
    """
    Assemble HydroShare Creator metadata dict
    :param creator_list: list of creator name string
    :return: HydroShare Creator metadata list
    """

    hs_creator_list = []
    for creator in creator_list:
        # if len(creator) > 28:  # criteria czo people said
        #     # organization
        #     hs_creator_list.append({'organization': creator, "name": ""})
        # elif len(creator) > 0:
        #     # person
        #     hs_creator_list.append({'name': creator})
        # else:
        #     hs_creator_list.append({'name': "Someone"})

        if len(creator) > 0:
            # person
            hs_creator_list.append({'name': creator})
        else:
            hs_creator_list.append({'name': "Someone"})

    # full metadata terms
    # {'organization': "", 'name': "Someone", 'email': "xxxx@czo.org", }

    return hs_creator_list


def get_files(component_files, migration_log=None, other_urls=[]):
    """
    This is a generator that returns a resource file dict in each iterate
    :param in_str: file field
    :return: None
    """
    file_name_used_dict = {}

    # # deal with readme.md file first to avoid potential naming conflict with component files
    # if os.path.isfile(readme_path):
    #     file_name_used_dict[README_FILENAME] = 0  # mark "readme.md" as used
    #     readme_file_info = {"file_type": "",
    #                         "path_or_url": readme_path,
    #                         "file_name": README_FILENAME,
    #                         "big_file_flag": False,
    #                         "file_size_mb": -1,
    #                         "original_url": "",
    #                         "metadata": {},
    #                         }
    #
    #     yield readme_file_info

    # loop through component files and metadata files
    for f_str in component_files.split("|"):
        try:
            f_info_list = f_str.split("$")
            f_location = f_info_list[0]
            f_topic = f_info_list[1]
            f_url = f_info_list[2].strip()
            f_data_level = f_info_list[3]
            f_private = f_info_list[4]
            f_doi = f_info_list[5]
            # TODO handle this in a more centralized way
            f_metadata_url = f_info_list[6].strip()

            ref_file_name = f_location + "-" + f_topic
            file_info = extract_fileinfo_from_url(f_url, ref_file_name,
                                                  file_name_used_dict=file_name_used_dict,
                                                  private_flag=(f_private.lower() == "y"))

            file_info["metadata"] = {"title": ref_file_name,
                                     #"spatial_coverage": {"name": f_location,},  # doesnt work without bounding box
                                     "extra_metadata": {"private": f_private,
                                                        "url": f_url,
                                                        "location": f_location,
                                                        "doi": f_doi,
                                                        },  # extra_metadata
                                     }
            yield file_info
        except Exception as ex:
            extra_msg = "Failed to parse resource file from component {}".format(f_str)
            log_exception(ex, migration_log=migration_log, extra_msg=extra_msg)
            yield 1

        try:
            metadata_file_info = extract_fileinfo_from_url(f_metadata_url, ref_file_name,
                                                           file_name_used_dict=file_name_used_dict,
                                                           skip_invalid_url=True)
            if metadata_file_info is None:
                yield 2
            else:
                metadata_file_info["metadata"]["extra_metadata"] = {"metadata_url": f_metadata_url}
                if file_info is not None:
                    metadata_file_info["metadata"]["title"] = "Metadata File for {}".format(file_info["file_name"])

                yield metadata_file_info
        except Exception as ex:
            extra_msg = "Failed to parse metadata file from component {}".format(f_str)
            log_exception(ex, migration_log=migration_log, extra_msg=extra_msg)
            yield 1

    # other urls - map/kml
    for url in other_urls:
        try:
            url = url.strip()
            ref_file_name = "map_or_kml"
            other_file_info = extract_fileinfo_from_url(url, ref_file_name,
                                                        file_name_used_dict=file_name_used_dict)
            other_file_info["metadata"]["extra_metadata"] = {"url": url}
            other_file_info["tag"] = "map"

            yield other_file_info
        except Exception as ex:
            extra_msg = "Failed to parse map/kml field {}".format(url)
            log_exception(ex, migration_log=migration_log, extra_msg=extra_msg)
            yield 1


def safe_get(url, timeout=10, headers=headers, stream=False, verify=True):
    """
    Attempts to retrieve resource at url
    :param url: url
    :param timeout: timeout
    :return:
    """
    r = {"url_asked": url, "status_code": 400, "error": "", "text": "", "history": ""}
    try:
        # sending headers is very important or in some cases requests.get() wont download the actual file content/binary
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


def _update_core_metadata(hs_obj, hs_id, metadata_dict, message=None, migration_log=None):
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
        log_exception(ex, migration_log=migration_log, extra_msg=extra_msg)
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


def get_metadata_list(csv_header, csv_value, output_header_list=None):

    header_list = csv_header.split('$') if output_header_list is None else output_header_list
    metadata_list = csv_value.split('|')
    md_list = []
    for md_terms in metadata_list:
        md_list.append(dict(zip(header_list, md_terms.split('$'))))

    return md_list


def _extract_value_from_df_row_dict(row_dict, key, required=True):
    v = str(row_dict.get(key)).strip()
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


def string_to_list(in_str, delimiter='|'):
    return in_str.split(delimiter) if in_str is not None else []


def create_hs_res_from_czo_row(czo_res_dict, czo_hs_account_obj, index=-99, ):
    """
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

    migration_log = {"success": False,
                   "czo_id": -1,
                   "hs_id": -1,
                   "ref_file_list": [],
                   "bad_ref_file_list": [],
                   "concrete_file_list": [],
                   "error_msg_list": [],
                   "uname": None,
                   "public": False,
                   "maps":[],
                   }

    _success = False
    try:
        # parse resource-level metadata
        # parse czo_id
        czo_id = _extract_value_from_df_row_dict(czo_res_dict, "czo_id")
        migration_log["czo_id"] = czo_id
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
        field_areas = _extract_value_from_df_row_dict(czo_res_dict, "FIELD_AREAS", required=False)
        field_areas_list = string_to_list(field_areas)
        location = _extract_value_from_df_row_dict(czo_res_dict, "location")

        # parse VARIABLES
        variables = _extract_value_from_df_row_dict(czo_res_dict, "VARIABLES", required=False)
        variables_list = string_to_list(variables)
        # parse VARIABLES-ODM2
        variables_odm2 = _extract_value_from_df_row_dict(czo_res_dict, "VARIABLES_ODM2", required=False)
        variables_odm2_list = string_to_list(variables_odm2)

        # parse date_start, date_end, date_range_comments
        date_start = _extract_value_from_df_row_dict(czo_res_dict, "date_start")
        date_end = _extract_value_from_df_row_dict(czo_res_dict, "date_end", required=False)
        date_range_comments = _extract_value_from_df_row_dict(czo_res_dict, "date_range_comments", required=False)

        # parse bouding box
        east_long = _extract_value_from_df_row_dict(czo_res_dict, "east_long")
        west_long = _extract_value_from_df_row_dict(czo_res_dict, "west_long")
        south_lat = _extract_value_from_df_row_dict(czo_res_dict, "south_lat")
        north_lat = _extract_value_from_df_row_dict(czo_res_dict, "north_lat")

        # parse citation, data_doi
        citation = _extract_value_from_df_row_dict(czo_res_dict, "citation", required=False)
        dataset_doi = _extract_value_from_df_row_dict(czo_res_dict, "dataset_doi", required=False)
        map_uploads = _extract_value_from_df_row_dict(czo_res_dict, "map_uploads", required=False)
        map_uploads_list = string_to_list(map_uploads)
        kml_files = _extract_value_from_df_row_dict(czo_res_dict, "kml_files", required=False)
        kml_files_list = string_to_list(kml_files)

        # parse TOPICS, KEYWORDS
        topics = _extract_value_from_df_row_dict(czo_res_dict, "TOPICS")
        topics_list = topics.split('|')
        keywords = _extract_value_from_df_row_dict(czo_res_dict, "KEYWORDS", required=False)
        keywords_list = string_to_list(keywords)

        # parse DISCIPLINES
        disciplines = _extract_value_from_df_row_dict(czo_res_dict, "DISCIPLINES", required=False)
        disciplines_list = string_to_list(disciplines)

        # parse sub_topic
        sub_topic = _extract_value_from_df_row_dict(czo_res_dict, "sub_topic", required=False)

        # parse EXTERNAL_LINKS-url$link_text,PUBLICATIONS_OF_THIS_DATA, RELATED_DATASETS
        external_links = _extract_value_from_df_row_dict(czo_res_dict, "EXTERNAL_LINKS-url$link_text", required=False)
        publications_of_this_data = _extract_value_from_df_row_dict(czo_res_dict, "PUBLICATIONS_OF_THIS_DATA",
                                                                    required=False)
        publications_using_this_data = _extract_value_from_df_row_dict(czo_res_dict, "PUBLICATIONS_USING_THIS_DATA",
                                                                       required=False)
        related_datasets = _extract_value_from_df_row_dict(czo_res_dict, "RELATED_DATASETS", required=False)
        related_datasets_list = string_to_list(related_datasets)

        # grants
        grants_csv_header = "AWARD_GRANT_NUMBERS-grant_number$funding_agency$url_for_grant"
        award_grants = _extract_value_from_df_row_dict(czo_res_dict,
                                                       grants_csv_header,
                                                       required=False)
        hs_award_grants_list = get_metadata_list(grants_csv_header,
                                                 award_grants,
                                                 output_header_list=["agency_name", "award_number", "agency_url"]) \
                                if award_grants is not None else []
        # patch hs_award_grants due to HS REST API bugs
        # add optional "award_title" as empty ""
        for grant in hs_award_grants_list:
            grant["award_title"] = ""

        # end parse resource-level metadata

        # hs title
        hs_res_title = title

        # hs abstract
        hs_res_abstract = ""
        hs_res_abstract += "{description}".format(description=description)

        # if subtitle is not None:
        #     hs_res_abstract += "\n\n{subtitle}".format(subtitle=subtitle)
        # hs_res_abstract += "\n\nCZO: {czos_list}".format(czos_list=", ".join(czos_list))
        # hs_res_abstract += "\n\nField Area: {field_areas_list}".format(field_areas_list=", ".join(field_areas_list))
        # hs_res_abstract += "\n\nLocation: {location}".format(location=location)

        # hs_res_abstract += "\n\nStart Date: {date_start}".format(date_start=date_start)
        # hs_res_abstract += "\nEnd Date: {date_end}".format(date_end=date_end if date_end is not None else "")
        if date_range_comments is not None:
            hs_res_abstract += "\nDate Range Comments: {date_range_comments}".format(
                date_range_comments=date_range_comments)
        # if citation is not None:
        #     hs_res_abstract += "\n\nCitation: {citation}".format(citation=citation)
        if dataset_doi is not None:
            hs_res_abstract += "\n\nDataset DOI: {dataset_doi}".format(dataset_doi=dataset_doi)
        # hs abstract end

        # TODO make sure this is all of them
        # hs keywords - czos, FIELD_AREAS, TOPICS, VARIABLES_ODM2, Keyword?????
        hs_res_keywords = [] + \
                            field_areas_list + \
                            topics_list + \
                            variables_odm2_list + \
                            czos_list

        hs_res_keywords = map(str.lower, hs_res_keywords)
        hs_res_keywords = set(hs_res_keywords)
        if "" in hs_res_keywords:
            hs_res_keywords.remove("")
        # hs keywords end

        # hs creator/author
        creator = _extract_value_from_df_row_dict(czo_res_dict, "creator", required=False)
        creator_list = string_to_list(creator)
        hs_creator_list = get_creator_hs_metadata(creator_list)
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
                                 description=description.replace('[CRLF]', ''),  # TODO verify
                                 variables=", ".join(variables_list),
                                 variables_odm2=", ".join(variables_odm2_list).replace('[CRLF]', ''),  # TODO verify
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
            hs_extra_metadata["comments"] = comments.replace('[CRLF]', ' \n\n')  # TODO verify
        if external_links is not None:
            external_links = external_links.split('|')
            external_links = ["{} | ".format(x.split('$')[0]) for x in external_links]
            external_links.insert(0, "| ")
            printable_links = " ".join(external_links)

            hs_extra_metadata["external_links"] = printable_links
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
                                  )
        migration_log["hs_id"] = hs_id
        migration_log["uname"] = "{}".format(hs.auth.username)  # export owner of this hs res
        logging.info('HS resource created at: {hs_id}'.format(hs_id=hs_id))

        # update Extended Metadata
        hs.resource(hs_id).scimeta.custom(hs_extra_metadata)

        # update Abstract/Description
        _success_abstract, _ = _update_core_metadata(hs, hs_id,
                                                     {"description": hs_res_abstract.replace('[CRLF]', '\n\n')}, # TODO verify change
                                                     message="Abstract",
                                                     migration_log=migration_log)

        # update Keywords/Subjects
        _success_keyword, _ = _update_core_metadata(hs, hs_id,
                                                    {"subjects": [{"value": kw} for kw in hs_res_keywords]},
                                                    message="Keyword",
                                                    migration_log=migration_log)

        # update creators
        _success_creator, _ = _update_core_metadata(hs, hs_id,
                                                    {"creators": hs_creator_list},
                                                    message="Author",
                                                    migration_log=migration_log)

        # update coverage
        # spatial coverage and period coverage must be updated at the same time
        # as updating any single one would remove the other
        _success_coverage, _ = _update_core_metadata(hs, hs_id,
                                                     {'coverages': [hs_coverage_spatial, hs_coverage_period]},
                                                     message="Coverage",
                                                     migration_log=migration_log)

        # update award grants
        _success_grants= True
        if len(hs_award_grants_list) > 0:
            _success_grants, _ = _update_core_metadata(hs, hs_id,
                                                       {'funding_agencies': hs_award_grants_list},
                                                       message="Funding_agencies",
                                                       migration_log=migration_log)

        # update relations for publication of this data
        _success_relations = True
        if publications_of_this_data is not None:
            hs_relations_list = [{
                    "type": "isDataFor",
                    "value": publications_of_this_data # TODO was capped at 499
                }]

            _success_relations, _ = _update_core_metadata(hs, hs_id,
                                                       {'relations': hs_relations_list},
                                                       message="Relations",
                                                       migration_log=migration_log)

        _success_file = True
        other_urls = [] + map_uploads_list + kml_files_list
        # component_files field
        component_files = czo_res_dict['COMPONENT_FILES-location$topic$url$data_level$private$doi$metadata_url']

        for f in get_files(component_files, migration_log=migration_log, other_urls=other_urls):
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
                    path_value = ""
                    kw = {"pid": hs_id, "path": path_value, "name": f['file_name'],
                          "ref_url": f['path_or_url'], "validate": False}
                    private_flag = f["file_name"].startswith("PRIVATE_")
                    try:
                        max_tries = 1 if private_flag else 4
                        resp_dict = retry_func(hs.createReferencedFile, max_tries=max_tries, kwargs=kw)
                        # log successful ref file
                        migration_log["ref_file_list"].append(f)
                    except Exception:
                        # change failing RefContentFile URL to HS homepage
                        kw["ref_url"] = "https://www.hydroshare.org/"
                        if not private_flag:
                            # add prefix "NOT_RESOLVING_URL_" to filename
                            kw["name"] = "NOT_RESOLVING_URL_{}".format(kw["name"])
                            # log not-resolving ref file
                            migration_log["bad_ref_file_list"].append(f)
                            _success_file = False
                        resp_dict = retry_func(hs.createReferencedFile, kwargs=kw)

                    file_id = resp_dict["file_id"]

                else:
                    # upload other files with auto file type detection
                    file_add_respone = hs.addResourceFile(hs_id, f["path_or_url"])

                    # file path in HS res
                    hs_file_path = file_add_respone["file_path"]

                    # record map files
                    if f["tag"] == "map" and hs_file_path.lower().endswith(('.jpg', '.jpeg', '.bmp', '.png')):
                        migration_log["maps"].append(hs_file_path)

                    try:
                        tmpfile_folder_path = os.path.dirname(f["path_or_url"])
                        assert(tmpfile_folder_path.startswith(tempfile.gettempdir()))
                        shutil.rmtree(tmpfile_folder_path)
                    except Exception:
                        pass

                    try:
                        # set Content Type to file
                        options = {
                            "file_path": hs_file_path,
                            "hs_file_type": "SingleFile"
                        }
                        hs.resource(hs_id).functions.set_file_type(options)

                        # This will be simplified by new hs_restclient PR
                        # find file id
                        #file_id = get_file_id_by_name(hs, hs_id, f["file_name"])
                        file_id = hs_file_path
                    except Exception:
                        pass

                    # log concrete file
                    migration_log["concrete_file_list"].append(f)

                hs.resource(hs_id).files.metadata(file_id, f["metadata"])
            except Exception as ex_file:
                _success_file = False
                extra_msg = "Failed upload file to HS {}: ".format(json.dumps(f))
                log_exception(ex_file, migration_log=migration_log, extra_msg=extra_msg)

        # make the resource public
        try:
            hs.setAccessRules(hs_id, public=True)
            logging.info("Resource is made Public")
            migration_log["public"] = True
        except Exception:
            logging.error("Failed to make Resource Public")

        # science_metadata_json = hs.getScienceMetadata(hs_id)
        # print (json.dumps(science_metadata_json, sort_keys=True, indent=4))

        logging.info("Done with NO.{index} CZO_ID: {czo_id}".format(index=index, czo_id=czo_id))
        if _success_abstract and _success_keyword and \
                _success_coverage and _success_creator and _success_file and \
                _success_grants and _success_relations:
            _success = True

    except Exception as ex:
        _success = False
        extra_msg = "Failed to migrate CZO dict {}: ".format(json.dumps(czo_res_dict))
        log_exception(ex, migration_log=migration_log, extra_msg=extra_msg)

    finally:
        migration_log["success"] = _success
        return migration_log
