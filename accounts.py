import logging
import pandas as pd

from hs_restclient import HydroShare, HydroShareAuthBasic


class HSAccount(object):

    def __init__(self, uname, pwd, hs_url, port, use_https, verify_https, *args, **kargs):
        self.uname = uname
        self.pwd = pwd
        self.hs_url = hs_url
        self.port = port
        self.use_https = use_https
        self.verify_https = verify_https

        self.hs_auth = self._get_hs_auth()
        self.hs = self._get_hs()

    def _get_hs_auth(self):

        auth = HydroShareAuthBasic(username=self.uname, password=self.pwd)
        return auth

    def _get_hs(self):

        try:
            return HydroShare(auth=self.hs_auth, hostname=self.hs_url,
                              port=self.port, use_https=self.use_https, verify=self.verify_https)
        except Exception as ex:
            logging.error(ex)


class CZOHSAccount(object):

    _uname_hs_dict = dict()

    def __init__(self, accounts_info):

        self._df = pd.DataFrame(accounts_info)
        for account_dict in accounts_info:
            hs_account = HSAccount(**account_dict)
            self._uname_hs_dict[account_dict["uname"]] = hs_account

    def get_hs_by_uname(self, uname):

        # uname -> hs obj
        return self._uname_hs_dict.get(uname).hs

    def get_hs_by_czo(self, czo):

        # czo -> uname
        uname = self.query("czo", czo, "uname", case_sensitive=False)
        # uname -> hs obj
        hs_account_info = self._uname_hs_dict.get(uname)
        if hs_account_info is None:
            logging.warning("Not found HS account for CZO {}".format(czo))
            hs_account_info = self._uname_hs_dict.get(self.get_uname_by_czo("default"))
        logging.info("Connecting to {} with account {}".format(hs_account_info.hs_url, hs_account_info.uname))

        return hs_account_info.hs

    def get_group_by_uname(self, uname):

        # uname -> group
        return self.query("uname", uname, "group")

    def get_uname_by_czo(self, czo):

        # czo -> uname
        return self.query("czo", czo, "uname", case_sensitive=False)

    def query(self, in_column, in_value, out_column, case_sensitive=True):

        if case_sensitive:
            row = self._df.loc[self._df[in_column] == in_value]
        else:
            row = self._df.loc[self._df[in_column].str.lower() == in_value.lower()]
        return row[out_column].values[0]
