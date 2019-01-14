import logging


def _update_core_metadata(hs_obj, res_id, metadata_dict, message=None):
    science_metadata_json = hs_obj.updateScienceMetadata(res_id, metadata=metadata_dict)
    if not message:
        message = str(metadata_dict)
    logging.info('{message} updated successfully'.format(message=message))
    return science_metadata_json
