import logging
import os
import tempfile
import uuid
import hashlib
from urllib.parse import unquote

import requests
import validators

from settings import BIG_FILE_SIZE_MB, MB_TO_BYTE, headers, USE_CACHED_FILES, CACHED_FILE_DIR
from util import retry_func


def check_file_size_mb(url):

    if USE_CACHED_FILES:
        _, f_size_byte = get_cached_file(url)
        if f_size_byte is not None:
            return f_size_byte / MB_TO_BYTE

    # sending headers is very important or in some cases requests.get() wont download the actual file content/binary
    res = requests.head(url, allow_redirects=True, headers=headers)
    f_size_str = res.headers.get('content-length')
    if f_size_str is None:
        logging.warning("Can't detect file size in HTTP header {}".format(url))
        return -999
    f_size_byte = float(f_size_str)
    f_size_mb = f_size_byte / MB_TO_BYTE
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

    if USE_CACHED_FILES:
        f_path, _ = get_cached_file(url)
        if f_path is not None:
            f_path = os.path.abspath(f_path)
            os.symlink(f_path, save_to)  # target must be a absolute path
            logging.info("Using local cache {} --> {}".format(save_to, f_path))
            return save_to

    # sending headers is very important or in some cases requests.get() wont download the actual file content/binary
    response = requests.get(url, stream=True, headers=headers)
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


def check_extension(filename):
    """
    check file extension and decide whether to harvest/download
    :param filename: filename
    :return: True: harvest/download;
    """
    filename = filename.lower()
    for ext in [".hdr", ".docx", ".csv", ".txt", ".pdf",
                ".xlsx", ".xls", ".dat", ".zip",  ".7z",
                ".kml",  ".kmz", ".rdb", ".jpg", ".jpeg",
                ".png"]:
        if filename.endswith(ext):
            return True
    return False


def handle_special_char(_fn):

    return _fn.replace(" ", "_").\
         replace(",", "_").\
         replace("/", "_").\
         replace("\\", "_").\
         replace('(', '').\
         replace(')', '')


def extract_fileinfo_from_url(f_url, ref_file_name,
                              file_name_used_dict=None, private_flag=False, skip_invalid_url=False):
    # case 1: Invalid url --> RefFileType (downstream codes will mark "NOT_RESOLVING")
    # case 2: Url ends with a filename with any supported extension and ...
    #   case 2-1: Big file --> RefFileType
    #   case 2-2: Not a big file --> SingleFileType
    #   case 2-3: Unknown size (missing headers) --> SingleFileType
    # case 3: Url ends with a filename without supported extension ---> RefFileType
    # case 4: Url has no explict filename ---> RefFileType
    # case 5: For case 2 ,3 ,4 if private_flag is True ---> RefFileType with prefix "Private_" in file_name

    ref_filetype = "ReferencedFile"
    regular_filetype = ""
    supported_extension = False
    big_file_flag = False
    path_or_url = f_url
    file_size_mb = -1

    if not validators.url(f_url):
        # case 1
        if skip_invalid_url:
            return None
        file_type = ref_filetype
        file_name = ref_file_name
    else:
        f_url_decoded = unquote(f_url)
        file_name = f_url_decoded.split("/")[-1]
        file_name = f_url_decoded.split("/")[-2] if len(file_name) == 0 else file_name
        supported_extension = check_extension(file_name)
        if supported_extension:
            # case 2-X
            file_size_mb = retry_func(check_file_size_mb, args=[f_url])
            big_file_flag = is_big_file(file_size_mb)
            if big_file_flag:
                # case 2-1
                file_type = ref_filetype
                file_name = file_name
            else:  # case 2-2, 2-3
                file_type = regular_filetype
                file_name = file_name
        else:  # case 3, 4
            file_type = ref_filetype
            file_name = ref_file_name

        # case 5
        if private_flag:
            file_type = ref_filetype
            file_name = "PRIVATE_{}".format(file_name)

    # remove special chars HS doesn't like in file name
    file_name = handle_special_char(file_name)
    # handel duplicate file_name
    file_name = _handle_duplicated_file_name(file_name, file_name_used_dict,
                                             split_ext=supported_extension)
    # download regular non-big-file to local
    if file_type == regular_filetype:
        path_or_url = retry_func(download_file, args=[f_url, file_name])

    file_info = {"file_type": file_type,
                 "path_or_url": path_or_url,
                 "file_name": file_name,
                 "big_file_flag": big_file_flag,
                 "file_size_mb": file_size_mb,
                 "original_url": f_url,
                 "metadata": {},
                 }
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


def hash_string(_str):
        hash_object = hashlib.md5(_str.encode())
        return hash_object.hexdigest()


def get_cached_file(url, base_dir=CACHED_FILE_DIR):
    hashkey = hash_string(url)
    f_path = os.path.join(base_dir, hashkey)
    if os.path.isfile(f_path):
        f_size = os.path.getsize(f_path)
        return f_path, f_size
    return None, None
