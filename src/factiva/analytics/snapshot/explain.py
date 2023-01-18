"""
  Classes to interact with the Snapshot Explain endpoint
"""
from .base import SnapshotBase, SnapshotBaseQuery, SnapshotBaseJobResponse
from ..auth import UserKey
from ..common import log, const, req
from .query import SnapshotQuery
import pandas as pd


class SnapshotExplain(SnapshotBase): # TODO: When identifying repetitive code, create a common class SnapshotBase
    """
    Class that interacts with the Factiva Snapshot Explain service.

    Parameters
    ----------
    user_key : str or UserKey
        String containing the 32-character long APi Key. If not provided, the
        constructor will try to obtain its value from the FACTIVA_USERKEY
        environment variable.
    query : str or SnapshotQuery, optional
        Query used to run any of the Snapshot-related operations. If a str is
        provided, a simple query with a `where` clause is created. If other
        query fields are required, either provide the SnapshotQuery object at
        creation, or set the appropriate object values after creation. This
        parameter is not compatible with snapshot_id.

    """

    __SAMPLES_BASEURL = f'{const.API_HOST}{const.API_EXTRACTIONS_BASEPATH}{const.API_EXTRACTIONS_SAMPLES_SUFFIX}'
    __MAX_SAMPLES = 100
    samples_response = None

    def __init__(
        self,
        user_key=None,
        query=None,
        job_id=None
    ):
        super().__init__(user_key=user_key)
        self.__log = log.get_factiva_logger()
        self.__JOB_BASE_URL = f'{const.API_HOST}{const.API_SNAPSHOTS_BASEPATH}'

        if job_id:
            self.__log.info(f'Creating SnapshotExplain instance with JobID {job_id}')
            self.job_response = SnapshotExplainJobReponse(job_id)
            self.get_job_response()

        elif query:
            if isinstance(query, str):
                self.query = SnapshotQuery(query)
            elif isinstance(query, SnapshotQuery):
                self.query = query
            else:
                raise ValueError('Unexpected query type')


    @log.factiva_logger
    def submit_job(self, payload=None):
        """
        Performs a POST request to the API using the assigned query to start
        an Explain job.

        If the job is initiated succesfully, results are assigned to the ``job_response``
        object. Otherwise any HTTP error will raise an exception.

        Returns
        -------
        Boolean : True if the submission was successful. An Exception otherwise.

        """
        self.__log.info('submit_job Start')
        if not self.query:
            raise ValueError('A query is needed to submit an Explain Job')

        headers_dict = {
                'user-key': self.user_key.key,
                'Content-Type': 'application/json'
            }
        
        submit_url = f'{self.__JOB_BASE_URL}{const.API_EXPLAIN_SUFFIX}'
        submit_payload = self.query.get_explain_query()

        response = req.api_send_request(method='POST', endpoint_url=submit_url, headers=headers_dict, payload=submit_payload)

        if response.status_code == 201:
            response_data = response.json()
            self.job_response = SnapshotExplainJobReponse(response_data["data"]["id"])
            self.job_response.job_state = response_data['data']['attributes']['current_state']
            self.job_response.job_link = response_data['links']['self']
        elif response.status_code == 400:
            raise ValueError(f'Invalid Query [{response.text}]')
        else:
            raise RuntimeError(f'API request returned an unexpected HTTP status, with content [{response.text}]')
        self.__log.info('submit_job End')
        return True


    @log.factiva_logger
    def get_job_response(self) -> bool:
        """
        Performs a request to the API using the job ID to get its status.

        If the job has been completed, results are assigned to the ``job_response`` object.

        Returns
        -------
        Boolean : True if the get request was successful. An Exception otherwise.

        """
        self.__log.info('get_job_response Start')
        # super().get_job_response_base()
        if (not self.job_response):
            raise RuntimeError('Job has not yet been submitted or Job ID was not set')

        headers_dict = {
            'user-key': self.user_key.key,
            'Content-Type': 'application/json'
        }

        self.__log.info(f'Requesting Explain Job info for ID {self.job_response.job_id}')
        getinfo_url = f'{self.__JOB_BASE_URL}/{self.job_response.job_id}{const.API_EXPLAIN_SUFFIX}'
        response = req.api_send_request(method='GET', endpoint_url=getinfo_url, headers=headers_dict)

        if response.status_code == 200:
            self.__log.info(f'Job ID {self.job_response.job_id} info retrieved successfully')
            response_data = response.json()
            self.job_response.job_state = response_data['data']['attributes']['current_state']
            self.job_response.job_link = response_data['links']['self']
            if self.job_response.job_state == const.API_JOB_DONE_STATE:
                self.job_response.volume_estimate = response_data['data']['attributes']['counts']
            # elif self.job_response.job_state == const.API_JOB_FAILED_STATE:
                # errors = response_data['errors']
                # raise RuntimeError(f"Job Failed with reason: {[e['title'] + e['detail'] for e in errors]}")
        elif response.status_code == 404:
            raise RuntimeError('Job ID does not exist.')
        elif response.status_code == 400:
            detail = response_data['errors'][0]['detail']
            raise ValueError(f'Bad Request: {detail}')
        else:
            raise RuntimeError(f'API request returned an unexpected HTTP status, with content [{response.text}]')
        self.__log.info('get_job_response End')
        return True


    def get_samples(self, num_samples:int=__MAX_SAMPLES):
        """
        Performs a request to the API using the job ID to get its status.

        If the job has been completed, results are assigned to the ``job_response`` object.

        Returns
        -------
        Boolean : True if the get request was successful. An Exception otherwise.

        """
        self.__log.info('get_samples Start')
        # super().get_job_response_base()
        if (not self.job_response):
            raise RuntimeError('Job has not yet been submitted or Job ID was not set')
        
        if (num_samples < 1) or (num_samples > self.__MAX_SAMPLES):
            raise ValueError(f'The n_samples value must be an integer between 1 and {self.__MAX_SAMPLES}')

        headers_dict = {
            'user-key': self.user_key.key,
            'Content-Type': 'application/json'
        }

        qs_parameters = {
            'num_samples': num_samples
        }

        self.__log.info(f'Requesting Samples for JobID {self.job_response.job_id}')
        samples_url = f'{self.__SAMPLES_BASEURL}/{self.job_response.job_id}'
        response = req.api_send_request(method='GET',
                                        endpoint_url=samples_url,
                                        headers=headers_dict,
                                        qs_params=qs_parameters)

        if response.status_code == 200:
            self.__log.info(f'Samples for Job ID {self.job_response.job_id} retrieved successfully')
            response_data = response.json()
            self.samples_response = SnapshotExplainSamplesResponse(response_data['data']['attributes']['sample'])
        elif response.status_code == 404:
            raise RuntimeError('Job ID does not exist.')
        elif response.status_code == 400:
            detail = response_data['errors'][0]['detail']
            raise ValueError(f'Bad Request: {detail}')
        else:
            raise RuntimeError(f'API request returned an unexpected HTTP status, with content [{response.text}]')
        self.__log.info('get_samples End')
        return True


    def __repr__(self):
        return super().__repr__()


    def __str__(self, detailed=True, prefix='  |-', root_prefix=''):
        ret_val = super().__str__(detailed, prefix, root_prefix)
        return ret_val


class SnapshotExplainQuery(SnapshotBaseQuery):
    pass


class SnapshotExplainJobReponse(SnapshotBaseJobResponse):

    volume_estimate = None


    def __repr__(self):
        return super().__repr__()


    def __str__(self, detailed=True, prefix='  |-', root_prefix=''):
        ret_val = super().__str__(detailed, prefix, root_prefix)
        if isinstance(self.volume_estimate, int):
            ret_val += f"{prefix}volume_estimate: {self.volume_estimate:,d}"
        else:
            ret_val += f"{prefix}volume_estimate: <NotCalculated>"
        return ret_val


class SnapshotExplainSamplesResponse():

    num_samples = None
    samples = None


    def __init__(self, samples_list:list) -> None:
        if not isinstance(samples_list, list):
            raise ValueError('Unexpected samples_list parameter.')

        self.samples = pd.DataFrame(samples_list)
        self.num_samples = len(samples_list)

    # Returns the following columns.
    # TODO: Create methods to split multi-value fields.
    # ['an', 'company_codes', 'company_codes_about', 'company_codes_occur',
    #    'industry_codes', 'ingestion_datetime', 'modification_datetime',
    #    'publication_datetime', 'publisher_name', 'region_codes',
    #    'region_of_origin', 'source_code', 'source_name', 'subject_codes',
    #    'title', 'word_count']
