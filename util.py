import math
import time
import tempfile
import os
from settings import README_FILENAME

def retry_func(fun, args=None, kwargs=None, max_tries=4, interval_sec=5, increase_interval=True, raise_on_failure=True):
    """
    TODO docstring
    :param fun:
    :param args:
    :param kwargs:
    :param max_tries:
    :param interval_sec:
    :param increase_interval:
    :return:
    """
    pass_on_args = args if args else []
    pass_on_kwargs = kwargs if kwargs else {}

    for i in range(max_tries):
        try:
            func_result = fun(*pass_on_args, **pass_on_kwargs)
            return func_result
        except Exception as ex:
            if i == max_tries - 1 and raise_on_failure:
                msg = "All {} attempts were failed to call {} with arguments: {} {}".format(max_tries,
                                                                                       str(fun),
                                                                                       str(pass_on_args),
                                                                                       str(pass_on_kwargs))
                raise Exception(msg)

            if increase_interval:
                time.sleep(interval_sec*(i+1))
            else:
                time.sleep(interval_sec)
            continue


def gen_readme(rowdata, cmap):
    """
    Create a readme from the mappings agreed on with CZOs and captured in markdown_map.json
    :param rowdata: dict data of row from csv
    :param cmap: mapping of column names, descriptions and csv column indecies
    :return: save markdown file to tmp
    """
    readme_path = os.path.join(tempfile.mkdtemp(), README_FILENAME)
    with open(os.path.join(readme_path), 'w', encoding='utf-8') as f:
        for key in cmap:
            info = rowdata[key]
            if key == 'COMPONENT_FILES-location$topic$url$data_level$private$doi$metadata_url':
                info = ' '.join(info.split('$'))
            try:
                if math.isnan(info) or not info:
                    pass
            except:
                f.write("### {}\n\n{}\n\n".format(cmap[key]['display'], info))
    return readme_path
