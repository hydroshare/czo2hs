import logging
import functools
import pandas as pd


def query_lookup_table(czo_id, lookup_data_df, attr="hs_id"):

        v = lookup_data_df.loc[czo_id][attr]
        if len(str(v)) == 0:
            return None
        return v


def second_pass(lookup_csv_path, czo_accounts):

    logging.info("\n\nSecond Pass Started")

    related_datasets_field_name = "related_datasets"
    # read lookup table and set czo_id as index
    lookup_data_df = pd.read_csv(lookup_csv_path, index_col=1)

    for index, row in lookup_data_df.iterrows():

        czo_id = index
        # get hs_id
        hs_id = query_lookup_table(czo_id, lookup_data_df)
        # get resource owner
        hs_owner = query_lookup_table(czo_id, lookup_data_df, attr="primary_owner")
        try:
            if None not in (hs_id, hs_owner):
                hs_owner = "default" if hs_owner == "czo" else hs_owner
                hs = czo_accounts.get_hs_by_czo(hs_owner)
                # get existing extended metadata
                extented_metadata = hs.resource(hs_id).scimeta.get()
                if related_datasets_field_name in extented_metadata:
                    czo_id_list = extented_metadata[related_datasets_field_name].split(', ')
                    czo_id_list = list(map(lambda x: int(str.strip(x)), czo_id_list))
                    hs_id_list = list(map(functools.partial(query_lookup_table, lookup_data_df=lookup_data_df), czo_id_list))
                    extented_metadata[related_datasets_field_name + "_hs"] = ", ".join(hs_id_list)
                    hs.resource(hs_id).scimeta.custom(extented_metadata)
                    logging.info("Updated {0} - {1} by account {2}".format(hs_id, czo_id, hs_owner))
        except Exception as ex:
            logging.error("Failed to updated {0} - {1} by account {2}: {3}".format(hs_id, czo_id, hs_owner, str(ex)))
    logging.info("Second Pass Done")
