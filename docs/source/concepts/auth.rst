.. _concepts_auth:

Authentication
==============

Depending on thte service intended to be used, an operation object will need either a
``UserKey`` or ``OAuthUser`` instance. As a best practice, it is recommended to use
ENV variables to store values to instantiate these objects (see Getting Started
> Environment Variables > :ref:`gettingstarted_envvariables_auth`).

UserKey
-------

Used by all services except the Article Retrieval Service. Usually it's not required to
be instantiated independently as the creation of a `parent` object will get the value
from the environment.

If using this class explicitly, the following code snippets can be found helpful:

.. code-block:: python

    from factiva.analytics import UserKey, SnapshotExplain
    u = UserKey('abcd1234abcd1234abcd1234abcd1234')
    se = SnapshotExplain(user_key=u)


OAuthUser
---------

Used by the Article Retrieval Service only. Like ``UserKey``, it is usually not required
to be instantiated independently. However, below code snippets can be helpful when using this
class explicitly:

.. code-block:: python

    from factiva.analytics import OAuthUser, ArticleRetrieval
    c_id = "0abcd1wxyz2abcd3wxyz4abcd5wxyz6o"
    uname = "9ZZZ000000-svcaccount@dowjones.com"
    pwd = "pa55WOrdpa55WOrd"
    ou = OAuthUser(client_id=c_id, username=uname, password=pwd)
    ar = ArticleRetrieval(oauth_user=ou)
    ...

When using ENV variables, the above snippet becomes shorter.

.. code-block:: python

    from factiva.analytics import SrticleRetrieval
    ar = ArticleRetrieval()
