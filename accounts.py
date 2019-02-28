import logging

from hs_restclient import HydroShare, HydroShareAuthBasic


class HSAccount(object):

    def __init__(self, uname, pwd, hs_url, port, use_https, verify_https):
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

    _czo_hs_dict = dict()

    def __init__(self, _czo_account_info_dict):

        for k, v in _czo_account_info_dict.items():
            hs_account = HSAccount(**v)
            self._czo_hs_dict[k] = hs_account

    def get_hs_by_czo(self, czo):

        hs_account_info = self._czo_hs_dict.get(czo.lower())
        if hs_account_info is None:
            logging.warning("Not found HS account for CZO {}".format(czo))
            hs_account_info = self._czo_hs_dict.get("default")
        logging.info("Connecting to {} with account {}".format(hs_account_info.hs_url, hs_account_info.uname))
        return hs_account_info.hs
