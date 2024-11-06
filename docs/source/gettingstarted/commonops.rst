Quick Operations
================


Check Account Statistics
------------------------

Assumes the ENV variable ``FACTIVA_USERKEY`` is set.

.. code-block:: python

    from factiva.analytics import AccountInfo
    u = AccountInfo()
    print(u)

.. code-block::

    <'factiva.analytics.AccountInfo'>
    ├─user_key: <'factiva.analytics.UserKey'>
    │  ├─key: ****************************1234
    │  └─cloud_token: **********************YKB22sJCkHXX
    ├─account_name: AccountName
    ├─account_type: account_with_contract_limits
    ├─active_product: DNA
    ├─max_allowed_concurrent_extractions: 1
    ├─max_allowed_extracted_documents: 2,200,000
    ├─max_allowed_extractions: 10
    ├─currently_running_extractions: 0
    ├─total_downloaded_bytes: 84,195,293
    ├─total_extracted_documents: 145,605
    ├─total_extractions: 3
    ├─total_stream_instances: 2
    ├─total_stream_subscriptions: 2
    ├─enabled_company_identifiers:
    │  ├─[1]: sedol
    │  ├─[3]: cusip
    │  ├─[4]: isin
    │  └─[5]: ticker_exchange
    ├─remaining_documents: 2.054,395
    └─remaining_extractions: 7


Get Account's Historical Full Extractions
-----------------------------------------

Uses the passed ``key`` parameter and ignores the ENV variable ``FACTIVA_USERKEY``.

.. code-block:: python

    from factiva.analytics import AccountInfo
    u = AccountInfo(key='abcd1234abcd1234abcd1234abcd1234')
    extractions = u.get_extractions()

The variable ``extractions`` will contain a Python ``list`` of ``SnapshotExtraction``
objects.


Get Volume Estimates With Snapshot Explain
------------------------------------------

Assumes the ENV variable ``FACTIVA_USERKEY`` is set.

.. code-block:: python

    from factiva.analytics import SnapshotExplain
    my_query = "publication_datetime >= '2020-01-01 00:00:00' AND LOWER(language_code) = 'en'"
    my_explain = SnapshotExplain(query=my_query)
    my_explain.process_job()  # This operation can take a few minutes to complete
    print(my_explain)

.. code-block::

    <'factiva.analytics.SnapshotExplain'>
    ├─user_key: <'factiva.analytics.UserKey'>
    │  ├─key: ****************************1234
    │  └─cloud_token: **********************YKB22sJCkHXX
    ├─query: <'factiva.analytics.SnapshotExplainQuery'>
    │  ├─where: publication_datetime >= '2023-01-01 00:00:00' AND UPPER(source_code) = 'DJDN'
    │  ├─includes: <NotSet>
    │  ├─excludes: <NotSet>
    │  ├─include_lists: <NotSet>
    │  └─exclude_lists: <NotSet>
    ├─job_response: <'factiva.analytics.SnapshotExplainJobResponse'>
    │  ├─job_id: 648075e7-b551-4bdb-b8f4-ed7f470ae6bd
    │  ├─job_link: https://api.dowjones.com/alpha/extractions/documents/648075e7-b551-4bdb-b8f4-ed7f470ae6bd/_explain
    │  ├─job_state: JOB_STATE_DONE
    │  ├─volume_estimate: 203,338
    │  └─errors: <NoErrors>
    └─samples: <NotRetrieved>

After its execution, the object ``my_explain.job_results`` contains details about the job itself and the estimated volume.


Get Extraction Details and Download Files
-----------------------------------------

Uses the passed ``key`` parameter and ignores the ENV variable ``FACTIVA_USERKEY``.

.. code-block:: python

    from factiva.analytics import SnapshotExtraction
    se = SnapshotExtraction('zmhsvx20tl')
    print(se)

.. code-block::

    <factiva.analytics.SnapshotExtraction'>
    ├─user_key: <'factiva.analytics.UserKey'>
    │  ├─key: ****************************1234
    │  └─cloud_token: **********************YKB22sJCkHXX
    ├─query: <NotRetrieved>
    └─job_response: <factiva.analytics.SnapshotExtractionJobReponse'>
        ├─job_id: dj-synhub-extraction-abcd1234abcd1234abcd1234abcd1234-zmhsvx20tl
        ├─job_link: https://api.dowjones.com/alpha/extractions/documents/dj-synhub-extraction-abcd1234abcd1234abcd1234abcd1234-zmhsvx20tl
        ├─job_state: JOB_STATE_DONE
        ├─short_id: zmhsvx20tl
        ├─files: <list> - [1] elements
        └─errors: <NoErrors>

.. code-block::

    se.download_files()

When the operation ends, files will be available in the local folder named as the ``short_id`` attribute (``zmhsvx20tl``).


Create a Streaming Instance
------------------------------------------

Assumes the ENV variable ``FACTIVA_USERKEY`` is set.

.. code-block:: python

    from factiva.analytics import StreamingInstance
    my_query = "publication_datetime >= '2020-01-01 00:00:00' AND LOWER(language_code) = 'en'"
    my_stream = StreamingInstance(query=my_query)
    my_stream.create()
    print(my_stream)

.. code-block::

    <'factiva.analytics.StreamingInstance'>
    ├─id: <Hidden>
    ├─short_id: 4doq2zigpf
    ├─user_key: <'factiva.analytics.UserKey'>
    │  ├─key: ****************************1234
    │  └─cloud_token: **********************YKB22sJCkHXX
    ├─query: "publication_datetime >= '2020-01-01 00:00:00' AND LOWER(language_code) = 'en'"
    ├─subscriptions:
    │  └─short_id: R4QwwB
    └─status: JOB_STATE_RUNNING

After its execution, the object ``my_explain.job_results`` contains details about the job itself and the estimated volume.