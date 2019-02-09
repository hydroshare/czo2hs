import logging
import os
import tempfile
import uuid
from urllib.parse import unquote

import requests
import validators

from settings import BIG_FILE_SIZE_MB
from util import retry_func


def check_file_size_mb(url):
    # res = requests.get(url, stream=True, allow_redirects=True)
    res = requests.head(url, allow_redirects=True)
    f_size_str = res.headers.get('content-length')
    if f_size_str is None:
        logging.warning("Can't detect file size in HTTP header {}".format(url))
        return -999
    f_size_byte = int(f_size_str)
    f_size_mb = f_size_byte / 1024.0 / 1024.0
    return f_size_mb


def download_file(url, file_name):
    """
       Download a remote czo file to local
       :param url: URL to remote CZ file
       :param save_to: local path to store the file
       :return: None
    """
    # TODO try catch and log
    # TODO handle for rate limiting
    save_to_base = tempfile.mkdtemp()
    save_to = os.path.join(save_to_base, file_name)
    response = requests.get(url, stream=True)
    with open(save_to, 'wb') as f:
        f.write(response.content)
    return save_to


def _append_rstr_to_fname(fn, split_ext=True, rstrl=6, pre_rstr=None):
    """
    append a small random str to filename: myfile_{RSTR}.txt
    :param fn: original filename
    :param split_ext: True - insert string before ext; False: append str to end
    :param pre_rstr: a string put prior to random string: myfile_{PRE_RSTR}_{RSTR}.txt
    :return: new filename
    """
    if rstrl > 32:
        logging.warning("Max length of random string is 32 characters")
    rstr = uuid.uuid4().hex[:rstrl]

    if isinstance(pre_rstr, str) and len(pre_rstr) > 0:
        rstr = "{}_{}".format(pre_rstr, rstr)

    if split_ext:
        file_name_base, file_name_ext = os.path.splitext(fn)
        fn_new = "{}_{}{}".format(file_name_base, rstr, file_name_ext)
    else:
        fn_new = "{}_{}".format(fn, rstr)
    return fn_new


def whether_to_harvest_file(filename):
    """
    check file extension and decide whether to harvest/download
    :param filename: filename
    :return: True: harvest/download;
    """
    filename = filename.lower()
    for ext in [".hdr", ".docx", ".csv", ".txt", ".pdf",
                ".xlsx", ".kmz", ".zip", ".xls", ".7z", ".kmz", ".dat", ".rdb"]:
        if filename.endswith(ext):
            return True
    return False


def extract_fileinfo_from_url(f_url, file_name_used_dict=None, ref_file_name=None, invalid_url_warning=False):
    file_info = None

    if not validators.url(f_url):
        if invalid_url_warning:
            raise Exception("Not a valid URL: {}".format(f_url))
        else:
            return file_info

    if file_name_used_dict is None:
        file_name_used_dict = {}

    if ref_file_name is None or len(ref_file_name) == 0:
        ref_file_name = "ref_file_{}".format(uuid.uuid4().hex[:6])

    f_url_decoded = unquote(f_url)
    file_name = f_url_decoded.split("/")[-1]
    if len(file_name) == 0:
        file_name = f_url_decoded.split("/")[-2]

    file_name = file_name.replace(" ", "_")

    harvestable_file_flag = whether_to_harvest_file(file_name)
    big_file_flag = False
    file_size_mb = -1
    if harvestable_file_flag:
        file_size_mb = retry_func(check_file_size_mb, args=[f_url])
        big_file_flag = is_big_file(file_size_mb)
        if big_file_flag:
            logging.warning("{} MB big file detected at {}".format(int(file_size_mb), f_url))

    if harvestable_file_flag and not big_file_flag:

        file_name = _handle_duplicated_file_name(file_name, file_name_used_dict)

        file_path_local = retry_func(download_file, args=[f_url, file_name])
        file_info = {"file_type": "",
                     "path_or_url": file_path_local,
                     "file_name": file_name,
                     "big_file_flag": False,
                     }

    else:  # Referenced File Type
        if not harvestable_file_flag:
            # url doesnt explicitly point to a file name
            file_name = ref_file_name.replace(" ", "_").replace(",", "_").replace("/", "_").replace("\\", "_")
            file_name = _handle_duplicated_file_name(file_name, file_name_used_dict, split_ext=False)
        else:
            # for harvestable but too big file
            file_name = _handle_duplicated_file_name(file_name, file_name_used_dict)
            big_file_flag = True

        file_info = {"file_type": "ReferencedFile",
                     "path_or_url": f_url,
                     "file_name": file_name,
                     "big_file_flag": big_file_flag,
                     }

    file_info["file_size_mb"] = file_size_mb
    file_info["original_url"] = f_url
    file_info["metadata"] = {}

    return file_info


def is_big_file(f_size_mb):
    if f_size_mb > BIG_FILE_SIZE_MB:
        return True
    return False


def _append_suffix_str_to_fname(fn, suffix_str, split_ext=True):
    """
    append a small random str to filename: myfile_{RSTR}.txt
    :param fn: original filename
    :param split_ext: True - insert string before ext; False: append str to end
    :param suffix_str: suffix string
    :return: new filename
    """
    suffix_str = str(suffix_str)
    if split_ext:
        file_name_base, file_name_ext = os.path.splitext(fn)
        fn_new = "{}_{}{}".format(file_name_base, suffix_str, file_name_ext)
    else:
        fn_new = "{}_{}".format(fn, suffix_str)
    return fn_new


def _handle_duplicated_file_name(file_name, file_name_used_dict, split_ext=True):
    file_name_new = file_name
    if file_name in file_name_used_dict:
        file_suffix_int = file_name_used_dict[file_name]
        file_suffix_int_new = file_suffix_int + 1
        file_name_new = _append_suffix_str_to_fname(file_name, file_suffix_int_new, split_ext=split_ext)
        file_name_used_dict[file_name] = file_suffix_int_new
    else:
        file_name_used_dict[file_name] = 0

    return file_name_new
