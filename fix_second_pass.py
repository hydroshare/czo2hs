import logging
import functools
import json
from collections import OrderedDict

import pandas as pd

from util import gen_readme
from settings import CZO_ACCOUNTS, CZO_DATA_CSV, README_COLUMN_MAP_PATH, \
    README_SHOW_MAPS, HS_EXTERNAL_FULL_DOMAIN, NEW_SECOND_PASS
from api_helpers import _extract_value_from_df_row_dict, string_to_list
from accounts import CZOHSAccount


def query_lookup_table(czo_id, lookup_data_df, attr="hs_id"):
    v = lookup_data_df.loc[czo_id][attr]
    if str(v) == "-1" or str(v).lower() == "nan" or len(str(v)) == 0:
        return None
    return v


def get_resource_file_url(hs_id, filename):
    landing = get_resource_landing_page_url(hs_id)
    file = "{landing}data/contents/{filename}".format(landing=landing,
                                                      filename=filename)
    return file


def get_resource_landing_page_url(hs_id):
    hs_res_url = "{HS_FULL_DOMAIN}/resource/{hs_id}/".format(HS_FULL_DOMAIN=HS_EXTERNAL_FULL_DOMAIN,
                                                             hs_id=hs_id)
    return hs_res_url


def build_maps_md(map_filename, hs_id):
    map_url = get_resource_file_url(hs_id, map_filename)
    md = "![alt text]({url} '{tooltip}')".format(url=map_url,
                                                 tooltip=map_filename)
    return md


def build_related_dataset_md(res_id, czo_id, czo_data_df=None):
    res_url = get_resource_landing_page_url(res_id)
    res_title = None
    if czo_data_df is not None:
        try:
            res_title = get_dict_by_czo_id(czo_id, czo_data_df)["title"]
        except:
            pass
    md = "[{res_title}]({url} '{tooltip}')".format(res_title=res_title if res_title is not None else res_id,
                                                   url=res_url,
                                                   tooltip=res_id)
    return md


def get_dict_by_czo_id(czo_id, czo_data_df):
    czo_row_dict = czo_data_df.loc[czo_data_df['czo_id'] == czo_id].to_dict(orient='records')[0]
    return czo_row_dict


def redo_second_pass(czo_csv_path, lookup_csv_path, czo_accounts):
    logging.info("\n\nFix Second Pass Started")

    # read czo csv
    czo_data_df = pd.read_csv(czo_csv_path)
    # read lookup table and set czo_id as index
    lookup_data_df = pd.read_csv(lookup_csv_path, index_col=1)

    readme_counter = 0
    ex_metadata_counter = 0

    with open(README_COLUMN_MAP_PATH) as f:
        readme_column_map = json.load(f, object_pairs_hook=OrderedDict)

    for index, row in lookup_data_df.iterrows():
        czo_id = index
        # get hs_id
        hs_id = query_lookup_table(czo_id, lookup_data_df)
        # get resource owner
        hs_owner = query_lookup_table(czo_id, lookup_data_df, attr="uname")
        public = query_lookup_table(czo_id, lookup_data_df, attr="public")
        maps = query_lookup_table(czo_id, lookup_data_df, attr="maps")

        if None not in (hs_id, hs_owner):

            logging.info("Updating {0} - {1} by account {2}".format(hs_id, czo_id, hs_owner))
            hs = czo_accounts.get_hs_by_uname(hs_owner)
            czo_row_dict = get_dict_by_czo_id(czo_id, czo_data_df)

            related_datasets_md = []
            try:  # update czo_id
                related_datasets = _extract_value_from_df_row_dict(czo_row_dict, "RELATED_DATASETS", required=False)
                if related_datasets is not None:
                    related_datasets_list = string_to_list(related_datasets)
                    print("Related datasets {}".format(related_datasets_list))
                    czo_id_list = list(map(lambda x: int(str.strip(x)), related_datasets_list))
                    hs_id_list = list(map(functools.partial(query_lookup_table, lookup_data_df=lookup_data_df),
                                          czo_id_list))

                    related_datasets_md = list(map(functools.partial(build_related_dataset_md,
                                                                     czo_data_df=czo_data_df), hs_id_list,
                                                                     czo_id_list))

                    # update czo_row_dict for readme.md
                    czo_row_dict["RELATED_DATASETS"] = "\n\r".join(related_datasets_md)

                    # get existing extended metadata
                    extented_metadata = hs.resource(hs_id).scimeta.get()
                    res_urls = list(map(get_resource_landing_page_url, hs_id_list))
                    extented_metadata["related_datasets_hs"] = ", ".join(res_urls)
                    hs.resource(hs_id).scimeta.custom(extented_metadata)
                    logging.info("Extended metadata")
                    ex_metadata_counter += 1
            except Exception as ex:
                logging.error(
                    "Failed to update related datasets {0} - {1}: {2}".format(hs_id, czo_id, str(ex)))

            # update maps
            try:
                if README_SHOW_MAPS and maps is not None:
                    maps_md = list(map(functools.partial(build_maps_md,
                                                         hs_id=hs_id),
                                                         maps.split('|')))
                    # update czo_row_dict for readme.md
                    czo_row_dict["map_uploads"] = "\n\r".join(maps_md)
            except Exception as ex:
                logging.error(
                    "Failed to process Maps for ReadMe {0} - {1}: {2}".format(hs_id, czo_id, str(ex)))

            # generate readme.md file
            if readme_column_map is not None:

                readme_path = gen_readme(czo_row_dict, related_datasets_md)

                try:
                    file_add_response = hs.addResourceFile(hs_id, readme_path)
                    logging.info("Creating ReadMe file {}".format(readme_path))
                    readme_counter += 1
                except IndexError as index_err:
                    logging.error('Resource exists hs: {} czo: {} - {} - {}'.format(hs_id, czo_id, readme_path, index_err))

            if not public:
                try:
                    hs.setAccessRules(hs_id, public=True)
                except Exception:
                    logging.error("Failed to make Resource Public")

    logging.info("Fix Pass Done: {} ex metadata updated; {} ReadMe files created\n\n".format(ex_metadata_counter,
                                                                                                readme_counter))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)-5.5s]  %(message)s",
        handlers=[logging.StreamHandler()])

    czo_accounts = CZOHSAccount(CZO_ACCOUNTS)
    redo_second_pass(CZO_DATA_CSV, NEW_SECOND_PASS, czo_accounts)
