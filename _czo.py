

def _update_core_metadata(hs_obj, res_id, metadata_dict):
    science_metadata_json = hs_obj.updateScienceMetadata(res_id, metadata=metadata_dict)
    return science_metadata_json
