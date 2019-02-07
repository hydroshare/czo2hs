from datetime import datetime as dt
import logging


def text_emphasis(text, char="*", num_char=20):
    """
    Add formatting emphasis to text with asterisks
    :param text: str
    :return: str
    """
    return char * num_char + str(text) + char * num_char


def elapsed_time(dt_start, return_type="log", prompt_str="Total Time Elapsed"):
    dt_utcnow = dt.utcnow()
    dt_timedelta = dt_utcnow - dt_start
    if return_type == "log":
        logging.info("{0}: {1}".format(prompt_str, dt_timedelta))
    elif return_type == "str":
        return str(dt_timedelta)
    else:
        return dt_timedelta


def prepare_logging_str(ex, attr, one_line=True):
    logging_str = attr + ": " + str(getattr(ex, attr, "NO " + attr))
    if one_line:
        logging_str = logging_str.replace("\r\n", " ")
        logging_str = logging_str.replace("\n", " ")
    return logging_str


def _log_exception(ex, record_dict=None, extra_msg=""):
    logging.error(text_emphasis("Error", char='!', num_char=10))

    ex_type = "type: " + str(type(ex))
    ex_doc = prepare_logging_str(ex, "__doc__")
    ex_msg = prepare_logging_str(ex, "message")
    ex_str = prepare_logging_str(ex, "__str__")
    if record_dict is not None:
        record_dict["error_msg_list"].append(extra_msg + ex_type + ex_doc + ex_msg + ex_str)

    logging.error(extra_msg)
    logging.error(ex_type + ex_doc + ex_msg + ex_str)
    logging.error(ex)
    logging.error(text_emphasis("", char='!', num_char=13))


def log_progress(progress_dict, header="Summary", start_time=None):
    """
    Write progress report to screen and logging file
    :param progress_dict: a dict contains progress info
    :param header: the header to print
    :return: None
    """

    error_counter = len(progress_dict["error"])
    success_counter = len(progress_dict["success"])
    logging.info(text_emphasis("{}".format(header)))
    logging.info("Total: {}; Success: {}; Error {}".format(error_counter + success_counter,
                                                           success_counter,
                                                           error_counter))

    if isinstance(start_time, dt):
        elapsed_time(start_time)


def log_uploaded_file_stats(record_dict):
    concrete_file_num = len(record_dict["concrete_file_list"])
    concrete_file_size_total = sum(
        [f["file_size_mb"] if f["file_size_mb"] > 0 else 0 for f in record_dict["concrete_file_list"]])
    logging.info("Uploaded concrete files: {}; Size {} MB".format(concrete_file_num, concrete_file_size_total))

    logging.info("Created ref files: {}".format(len(record_dict["ref_file_list"])))
    ref_big_file_list = list(filter(lambda f: (f["big_file_flag"] == True), record_dict["ref_file_list"]))
    logging.info("Big ref files list:".format(len(ref_big_file_list)))
    for f_big in ref_big_file_list:
        logging.info(f_big)