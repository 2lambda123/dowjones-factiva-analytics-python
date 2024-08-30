"""
This module contains classes and tools to interact with account-related
endpoints available in Factiva Analytics APIs.
"""
import json
import pandas as pd
from typing import Union
from ..common import log, req, tools, const, config
from ..auth import UserKey
from ..snapshots import SnapshotExtractionList
from ..streams import StreamingInstanceList

class AccountInfo:
    """
    Class that represents a user-key Account and can be instantiated based on the
    user-key value provided by the Dow Jones Developer Support team.

    """

    __API_ENDPOINT_BASEURL = f'{const.API_HOST}{const.API_ACCOUNT_BASEPATH}/'
    __log = None
    
    user_key: UserKey = None
    account_name: str = None
    account_type: str = None
    active_product: str = None
    max_allowed_concurrent_extractions: int = None
    max_allowed_extracted_documents: int = None
    max_allowed_extractions: int = None
    currently_running_extractions: int = None
    total_downloaded_bytes: int = None
    total_extracted_documents: int = None
    total_extractions: int = None
    total_stream_instances: int = None
    total_stream_subscriptions: int = None
    enabled_company_identifiers: list = None
    streams_list: StreamingInstanceList = None
    extractions_list: SnapshotExtractionList = None


    def __init__(self, user_key: Union[UserKey, str]):
        """
        Construct the instance of the class
        
        Parameters
        ----------
        key : str
            String containing the 32-character long APi Key. If not provided, the
            constructor will try to obtain its value from the ``FACTIVA_USERKEY``
            environment variable.
        stats : bool
            Indicates if user data has to be pulled from the server at creation
            time (``True``) or just create an instance with no stats data
            (``False`` - default). This operation fills account detail properties
            along with maximum, used and remaining values. It may take several 
            seconds to complete.
        
        Examples
        --------
        Creating a new UserKey instance providing the ``key`` string explicitly and
        retrieving the latest account details:

        .. code-block:: python

            from factiva.analytics import UserKey
            u = UserKey('abcd1234abcd1234abcd1234abcd1234', True)
            print(u)

        .. code-block::

            <class 'factiva.analytics.UserKey'>


        Creating a new instance taking the key value from the ``FACTIVA_USERKEY``
        environment varaible, and not requesting account statistics.

        .. code-block:: python

            from factiva.analytics import UserKey
            u = UserKey()
            print(u)

        .. code-block::

            <class 'factiva.analytics.UserKey'>

        """
        self.__log = log.get_factiva_logger()
        self.user_key = UserKey(user_key)
        self.get_stats()
        self.get_extractions()
        self.get_streams(running=False)


    @property
    def remaining_extractions(self):
        """
        Dynamic property that calculates the account's remaining extractions
        """
        if self.max_allowed_extractions:
            return self.max_allowed_extractions - self.total_extractions
        return None


    @property
    def remaining_documents(self):
        """
        Dynamic property that calculates the account's remaining documents
        """
        if self.max_allowed_extracted_documents:
            return self.max_allowed_extracted_documents - self.total_extracted_documents
        return None


    @log.factiva_logger()
    def get_stats(self) -> bool:
        """
        Request the account details to the Factiva Account API Endpoint.
        This operation can take several seconds to complete.

        Returns
        -------
        bool:
            ``True`` if the operation was completed successfully. ``False``
            otherwise. All returned values are assigned to the object's 
            properties directly.

        """
        self.__log.info('get_stats started')
        account_endpoint = f'{self.__API_ENDPOINT_BASEURL}{self.user_key.key}'
        req_head = {'user-key': self.user_key.key}
        resp = req.api_send_request(method='GET', endpoint_url=account_endpoint, headers=req_head)
        if resp.status_code == 200:
            try:
                resp_obj = json.loads(resp.text)
                self.account_name = resp_obj['data']['attributes']['name']
                self.account_type = resp_obj['data']['type']
                self.active_product = resp_obj['data']['attributes']['products']
                self.max_allowed_concurrent_extractions = resp_obj['data']['attributes']['max_allowed_concurrent_extracts']
                self.max_allowed_extracted_documents = resp_obj['data']['attributes']['max_allowed_document_extracts']
                self.max_allowed_extractions = resp_obj['data']['attributes']['max_allowed_extracts']
                self.currently_running_extractions = resp_obj['data']['attributes']['cnt_curr_ext']
                self.total_downloaded_bytes = resp_obj['data']['attributes']['current_downloaded_amount']
                self.total_extracted_documents = resp_obj['data']['attributes']['tot_document_extracts']
                self.total_extractions = resp_obj['data']['attributes']['tot_extracts']
                self.total_stream_instances = resp_obj['data']['attributes']['tot_topics']
                self.total_stream_subscriptions = resp_obj['data']['attributes']['tot_subscriptions']
                self.enabled_company_identifiers = resp_obj['data']['attributes']['enabled_company_identifiers']
            except Exception as error:
                raise AttributeError('Unexpected Account Information API Response.') from error
        elif resp.status_code == 403:
            raise ValueError('Factiva User-Key does not exist or inactive.')
        else:
            raise RuntimeError('Unexpected Account Information API Error')
        self.__log.info('get_stats ended')
        return True


    @log.factiva_logger()
    def get_extractions(self, updates=False) -> SnapshotExtractionList:
        """
        Request a list of historical extractions for the account.

        Parameters
        ----------
        updates : bool
            Indicates whether the retrieved list should include update
            operations (``True``) or not (``False`` - default).

        Returns
        -------
        padas.Dataframe:
            containing the list of historical extractions for the account.

        """
        self.__log.info('get_extractions started')
        endpoint = f'{const.API_HOST}{const.API_EXTRACTIONS_BASEPATH}'

        headers_dict = {'user-key': self.user_key.key}

        response = req.api_send_request(method='GET', endpoint_url=endpoint, headers=headers_dict)

        if response.status_code != 200:
            if response.status_code == 403:
                raise ValueError('Factiva API-Key does not exist or inactive.')

            raise RuntimeError(f'Unexpected API Error with message: {response.text}')

        response_data = response.json()
        if response_data['data'] == []:
            extraction_df = pd.DataFrame()
        else:
            extraction_df = pd.DataFrame([tools.flatten_dict(extraction) for extraction in response_data['data']])
            extraction_df.rename(columns={'id': 'object_id'}, inplace=True)
            ids_df = extraction_df['object_id'].str.split('-', expand=True)

            if ids_df.shape[1] >= 5:
                extraction_df['short_id'] = ids_df[4]
            else:
                extraction_df['short_id'] = None

            if ids_df.shape[1] >= 7:
                extraction_df['update_id'] = ids_df[6]
            else:
                extraction_df['update_id'] = None

            extraction_df.drop(['self', 'type'], axis=1, inplace=True)

            if not updates:
                extraction_df = extraction_df.loc[extraction_df.update_id.isnull()]

        self.extractions_list = SnapshotExtractionList(extraction_df)
        self.__log.info('get_extractions ended')
        return self.extractions_list


    @log.factiva_logger()
    def get_streams(self, running=True) -> pd.DataFrame:      # TODO: Change return type
        """
        Retrieves the list of streams for the user.

        Parameters
        ----------
        running : bool
            Indicates whether the retrieved list should be restricted
            to only running streams (``True`` - default) or also include
            historical ones (``False``).

        Returns
        -------
        pandas.DataFrame:
            DataFrame with the list of historical extractions

        """
        self.__log.info('get_streams started')
        request_headers = {'user-key': self.user_key.key}
        response = req.api_send_request(
            method="GET",
            endpoint_url=f'{const.API_HOST}{const.API_STREAMS_BASEPATH}',
            headers=request_headers
        )
        if response.status_code == 200:
            try:
                def extract_subscriptions(subscription):
                    id_list = []
                    for i in subscription:
                        s_idp = i['id'].split('-')
                        s_id = f"{s_idp[-3]}-{s_idp[-2]}-{s_idp[-1]}"
                        id_list.append(s_id)
                    return id_list

                response_data = response.json()
                stream_df = pd.DataFrame([tools.flatten_dict(extraction) for extraction in response_data['data']])
                stream_df.rename(columns={'id': 'stream_id'}, inplace=True)
                ids_df = stream_df['stream_id'].str.split('-', expand=True)
                stream_df['short_id'] = ids_df[4]
                stream_df['stream_type'] = ids_df[2]
                stream_df['subscriptions'] = stream_df['data'].apply(extract_subscriptions)
                stream_df['n_subscriptions'] = stream_df['subscriptions'].str.len()
                stream_df.drop(['self', 'type', 'data'], axis=1, inplace=True)

                if running:
                    stream_df = stream_df.loc[stream_df.job_status == const.API_JOB_RUNNING_STATE]
            except Exception as error:
                raise AttributeError('Unexpected Get Streams API Response.') from error
        elif response.status_code == 404:
            stream_df = pd.DataFrame()
        elif response.status_code == 403:
            raise ValueError('Factiva API-Key does not exist or inactive.')
        else:
            raise RuntimeError('Unexpected Get Streams API Error')
        
        self.streams_list = StreamingInstanceList(stream_df)
        self.__log.info('get_streams ended')
        return stream_df


    def is_active(self) -> bool:
        request_headers = {'user-key': self.user_key.key}
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
        pprop = self.__dict__.copy()
        del pprop['_AccountInfo__log']
        del pprop['user_key']
        del pprop['account_name']
        del pprop['account_type']
        del pprop['active_product']
        del pprop['enabled_company_identifiers']
        
        ret_val = f"{root_prefix}<'factiva.analytics.{str(self.__class__).split('.')[-1]}"
        ret_val += f"\n{prefix}user_key: {self.user_key.__str__(detailed=False, prefix='  │  ├─')}"
        ret_val += f"\n{prefix}account_name: {tools.print_property(self.account_name, '<NotLoaded>')}"
        ret_val += f"\n{prefix}account_type: {tools.print_property(self.account_type, '<NotLoaded')}"
        ret_val += f"\n{prefix}active_product: {tools.print_property(self.active_product, '<NotLoaded>')}\n"
        ret_val += "\n".join((f"{prefix}{item}: {tools.print_property(pprop[item], '<NotLoaded>')}" for item in pprop))
        ret_val += f"\n{prefix}enabled_company_identifiers:"
        if len(self.enabled_company_identifiers) >= 1:
            ci_list = [f"\n{prefix.replace('├', '│')[0:-1]}  ├─[{ci['id']}]: {ci['name']}" for ci in self.enabled_company_identifiers]
            ci_list.sort()
            ci_list[-1] = ci_list[-1].replace('├', '└')
            for ci in ci_list:
                ret_val += ci
        else:
            ret_val += f"\n{prefix.replace('├', '│')[0:-1]}  └─<NotLoaded>"
        ret_val += f"\n{prefix}remaining_documents: {tools.print_property(self.remaining_documents, '<NotLoaded>')}"
        ret_val += f"\n{prefix[0:-2]}└─remaining_extractions: {tools.print_property(self.remaining_extractions, '<NotLoaded>')}"

        return ret_val

