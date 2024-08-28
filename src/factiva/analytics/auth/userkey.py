"""
This module contains classes and tools to manage UserKey objects lifecycle.
UserKey is the most used authentication method within
Factiva Analytics APIs.
"""
import json
# import pandas as pd
from ..common import log, req, tools, const, config


class UserKey:
    """
    Class that represents an API user and can be instantiated based on the
    user-key value provided by the Dow Jones Developer Support team.

    """

    # __API_ENDPOINT_BASEURL = f'{const.API_HOST}{const.API_ACCOUNT_BASEPATH}/'
    __API_CLOUD_TOKEN_URL = f'{const.API_HOST}{const.API_ACCOUNT_STREAM_CREDENTIALS_BASEPATH}'
    __log = None
    
    key: str = None
    cloud_token: dict = None


    def __init__(self, key=None):
        """
        Construct the instance of the class
        
        Parameters
        ----------
        key : str
            String containing the 32-character long APi Key. If not provided, the
            constructor will try to obtain its value from the ``FACTIVA_USERKEY``
            environment variable.

        """
        self.__log = log.get_factiva_logger()
        if key is None:
            try:
                key = config.load_environment_value('FACTIVA_USERKEY')
            except Exception as error:
                raise ValueError(
                    'key parameter not provided and environment variable FACTIVA_USERKEY not set.'
                ) from error

        if len(key) != 32:
            raise ValueError('Factiva User-Key has the wrong length')

        self.key = key
        if self.is_active():
            self.get_cloud_token()
        else:
            raise ValueError('Factiva User-Key does not exist or inactive.')


    @log.factiva_logger()
    def get_cloud_token(self) -> bool:
        """
        Request a cloud token and stores its content in the ``cloud_token``
        property

        Returns
        -------
        bool:
            ``True`` if the operation was completed successfully. ``False``
            otherwise.

        """
        self.__log.info('get_cloud_token started')
        req_head = {'user-key': self.key}
        response = req.api_send_request(
            method="GET",
            endpoint_url=f'{self.__API_CLOUD_TOKEN_URL}',
            headers=req_head
        )

        if response.status_code == 401:
            message = '''
                Extraction API authentication failed for given
                credentials header:{}
                '''.format(req_head)
            raise RuntimeError(message)
        try:
            streaming_credentials_string = response.json()['data']['attributes']['streaming_credentials']
        except TypeError as type_error:
            raise ValueError('Unable to get a cloud token for the given key. This account might have limited access.') from type_error

        if streaming_credentials_string is not None:
                self.cloud_token = json.loads(streaming_credentials_string)
        self.__log.info('get_cloud_token ended')
        return True


    def is_active(self) -> bool:
        request_headers = {'user-key': self.key}
        response = req.api_send_request(
            method="GET",
            endpoint_url=f'{const.API_HOST}{const.API_SNAPSHOTS_TAXONOMY_BASEPATH}',
            headers=request_headers
        )
        if response.status_code == 200:
            return True
        else:
            return False


    def __repr__(self):
        """Return a string representation of the object."""
        return self.__str__()


    def __str__(self, detailed=True, prefix='  ├─', root_prefix=''):
        masked_key = tools.mask_string(self.key)
        
        if not self.cloud_token:
            masked_token = '<NotLoaded>'
        else:
            masked_token = tools.mask_string(self.cloud_token['private_key'][58:92], 12)

        ret_val = f"{root_prefix}<'factiva.analytics.{str(self.__class__).split('.')[-1]}"
        ret_val += f'\n{prefix}key: {masked_key}'
        ret_val += f'\n{prefix[0:-2]}└─cloud_token: {masked_token}'

        return ret_val

