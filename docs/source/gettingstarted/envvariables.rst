.. _gettingstarted_envvariables:

Environment Variables
=====================

When a class is instantiated, depending on the functionality some Environment Variables might be
required unless a value is specified explicitly in the code.


.. _gettingstarted_envvariables_auth:

Authentication
--------------

UserKey
^^^^^^^

* ``FACTIVA_USERKEY``: Assigned API user key. E.g. ``abcd1234abcd1234abcd1234abcd1234``.
    Used in all services except ArticleRetrieval.

OAuthUser
^^^^^^^^^

* ``FACTIVA_CLIENTID``: Assigned OAuth Client ID. E.g. ``0abcd1wxyz2abcd3wxyz4abcd5wxyz6o``.
    Required for ArticleRetrieval.
* ``FACTIVA_USERNAME``: Assigned OAuth Username. E.g. ``9ZZZ000000-svcaccount@dowjones.com``.
    Required for ArticleRetrieval.
* ``FACTIVA_PASSWORD``: Assigned OAuth Password. E.g. ``pa55WOrdpa55WOrd``.
    Required for ArticleRetrieval.


.. _gettingstarted_envvariables_snapshots:

Snapshots & Streams
-------------------

* ``FACTIVA_WHERE``: Query where statement that will be used when creating a new Snapshots
    or Streams object with no where/query parameter.
* ``FACTIVA_SUBSCRIPTIONID``: Subscription ID from an existing Streaming Instance. E.g.
    ``dj-synhub-stream-abcd1234abcd1234abcd1234abcd1234-1234abcxyz-filtered-abc123``.


.. _gettingstarted_envvariables_logging:

Logging
-------

* ``FACTIVA_LOGLEVEL``: Level of detail for the logs.
    Accepted values are ``DEBUG``, ``INFO`` (`default`), ``WARNING``, ``ERROR``, ``CRITICAL``.



Handlers and Data Processing
----------------------------

Elasticsearch
^^^^^^^^^^^^^

ENV variables used in Elasticsearch.


BigQuery
^^^^^^^^
ENV variables used in BigQuery.


