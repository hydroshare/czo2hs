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
        info += "  \n<br /><br />\n  "

        info += "------\n##SUBJECTS\n"
        info += normal_write("Disciplines", rowdata.get('DISCIPLINES'))
        info += normal_write("Topics", rowdata.get('TOPICS'))
        info += conditional_write('Subtopic', rowdata.get('sub_topic'))
        info += normal_write("Keywords", rowdata.get('KEYWORDS'))
        info += normal_write("Variables", rowdata.get('VARIABLES'))
        info += normal_write("Variables ODM2", rowdata.get('VARIABLES_ODM2'))
        info += "  \n<br /><br />\n  "

        info += "------\n##TEMPORAL\n"
        info += normal_write("Date Start", rowdata.get('date_start'))
        info += normal_write("Date End", rowdata.get('date_end'))
        info += conditional_write('Date Range Comments', rowdata.get('date_range_comments'))
        info += "  \n<br /><br />\n  "

        info += "------\n##SPATIAL\n"
        info += normal_write("Field Areas", rowdata.get('FIELD_AREAS'))
        info += normal_write("Location", rowdata.get('location'))
        info += normal_write("North latitude", rowdata.get('north_lat'))
        info += normal_write("South latitude", rowdata.get('south_lat'))
        info += normal_write("West longitude", rowdata.get('west_long'))
        info += normal_write("East longitude", rowdata.get('east_long'))
        info += "  \n<br /><br />\n  "

        info += "------\n##REFERENCE\n"
        info += conditional_write("Citation", rowdata.get('citation'))
        _pub_of = str(rowdata.get('PUBLICATIONS_OF_THIS_DATA'))
        if _pub_of.lower() != 'nan' and _pub_of.lower() != 'none':
            _pub_of = _pub_of.replace('|', '\n\n')
            info += conditional_write('Publications of this data', _pub_of)

        _pub_using = str(rowdata.get('PUBLICATIONS_USING_THIS_DATA'))
        if _pub_using.lower() != 'nan' and _pub_using.lower() != 'none':
            _pub_using = _pub_using.replace('|', '\n\n')
            info += conditional_write('Publications using this data', _pub_using)
        info += normal_write("CZO ID", rowdata.get('czo_id'))
        info += conditional_write('Related datasets', rowdata.get('RELATED_DATASETS'))

        ext_links = str(rowdata.get('EXTERNAL_LINKS-url$link_text'))
        if ext_links.lower() != 'nan' and ext_links.lower() != 'none':
            ext_links = ext_links.split('|')
            ext_links = ["<a href='{}' target='_blank'>{}</a> | ".format(x.split('$')[0], x.split('$')[1]) for x in ext_links]
            printable_links = " ".join(ext_links)
            info += conditional_write('External Links', printable_links)

        award_grants = str(rowdata.get('AWARD_GRANT_NUMBERS-grant_number$funding_agency$url_for_grant'))
        if award_grants.lower() != 'nan' and award_grants.lower() != 'none':
            award_grants = award_grants.split('|')
            award_grants = ["<a href='{}' target='_blank'>{} - {}</a>".format(x.split('$')[2], x.split('$')[0], x.split('$')[1]) for x in award_grants]
            printable_award_grants = " \n\n".join(award_grants)
            info += conditional_write('Award Grant Numbers', printable_award_grants)
            
        info += "  \n<br /><br />\n  "

        _comments = str(rowdata.get('comments'))
        if _comments and _comments.lower() != 'nan' and _comments.lower() != 'none':
            info += "------\n##COMMENTS\n"
            info += conditional_write('Comments', _comments.replace('[CRLF]', '\n\n'))
        f.write(info)
    return readme_path
