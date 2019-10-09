import math
import time
import tempfile
import os
from settings import README_FILENAME


def retry_func(fun, args=None, kwargs=None, max_tries=4, interval_sec=5, increase_interval=True, raise_on_failure=True):
    """
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


def conditional_write(_heading, _text):
    """
    conditionally output if exists and not a stringified empty token
    :param _text:
    :return:
    """
    _text = str(_text)
    if _text:
        if _text.lower() != 'nan' and _text.lower() != 'none':
            return "###{}\n".format(_heading) + _text.replace('[CRLF]', '\n') + "\n\n"
    return ""


def normal_write(_heading, _text):
    """
    write a line for readme
    :return:
    """
    _text = str(_text)
    return "###{}\n".format(_heading) + _text.replace('[CRLF]', '\n') + "\n\n"


def gen_readme(rowdata, related_resources):
    """
    Create a readme from the mappings agreed on with CZOs and captured in markdown_map.json
    :param rowdata: dict data of row from csv
    :param related_resources: list of hydroshare resource ids
    :return: save markdown file to tmp
    """
    readme_path = os.path.join(tempfile.mkdtemp(), README_FILENAME)
    info = ''
    with open(os.path.join(readme_path), 'w', encoding='utf-8') as f:
        info += "#" + rowdata.get('title') + "\n"
        info += "------\n##OVERVIEW\n"
        info += normal_write("Description/Abstract", rowdata.get('description'))
        info += conditional_write('Dataset DOI', rowdata.get('dataset_doi'))
        info += normal_write("Creator/Author", rowdata.get('creator'))
        info += normal_write("CZOs", rowdata.get('CZOS'))
        info += normal_write("Contact", rowdata.get('contact'))
        info += conditional_write('Subtitle', rowdata.get('subtitle'))

        info += "------\n##SUBJECTS\n"
        info += normal_write("Disciplines", rowdata.get('DISCIPLINES'))
        info += normal_write("Topics", rowdata.get('TOPICS'))
        info += conditional_write('Subtopic', rowdata.get('sub_topic'))
        info += normal_write("Keywords", rowdata.get('KEYWORDS'))
        info += normal_write("Variables", rowdata.get('VARIABLES'))
        info += normal_write("Variables ODM2", rowdata.get('VARIABLES_ODM2'))

        info += "------\n##TEMPORAL\n"
        info += normal_write("Date Start", rowdata.get('date_start'))
        info += normal_write("Date End", rowdata.get('date_end'))
        info += conditional_write('Date Range Comments', rowdata.get('date_range_comments'))

        info += "------\n##SPATIAL\n"
        info += normal_write("Field Areas", rowdata.get('FIELD_AREAS'))
        info += normal_write("Location", rowdata.get('location'))
        info += normal_write("North latitude", rowdata.get('north_lat'))
        info += normal_write("South latitude", rowdata.get('south_lat'))
        info += normal_write("West longitude", rowdata.get('west_long'))
        info += normal_write("East longitude", rowdata.get('east_long'))

        info += "------\n##REFERENCE\n"
        info += conditional_write("Citation", rowdata.get('citation'))
        info += conditional_write('Publications of this data', rowdata.get('PUBLICATIONS_OF_THIS_DATA'))
        info += conditional_write('Publications using this data', rowdata.get('PUBLICATIONS_USING_THIS_DATA'))
        info += normal_write("CZO ID", rowdata.get('czo_id'))
        info += conditional_write('Related datasets', rowdata.get('RELATED_DATASETS'))
        # info += conditional_write('Related Resources', related_resources)
        info += conditional_write('External Links', rowdata.get('EXTERNAL_LINKS-url'))
        info += conditional_write('Award Grant Numbers', rowdata.get('AWARD_GRANT_NUMBERS-grant_number'))

        if rowdata.get('comments'):
            info += "------\n##COMMENTS\n"
            info += conditional_write('Comments', rowdata.get('comments'))
        f.write(info)
    return readme_path
