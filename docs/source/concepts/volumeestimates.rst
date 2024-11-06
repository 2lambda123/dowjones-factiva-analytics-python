Volume Estimates
================

Accurate volume estimates are based on the Snapshot Explain operation. This
operation returns the exact number of matching articles in the archive.

.. code-block:: python

    from factiva.analytics import SnapshotExplain
    where_str = "publication_datetime >= '2020-01-01 AND language_code = 'en' AND REGEXP_CONTAINS(industry_codes, r'(?i)(^|,)(i1|i25121|i2567)($|,)')"
    se = SnapshotExplain(query=where_str)
    se.process_job()
    print(f"The query matches {se.job_results.volume_estimate} articles")


.. code-block::

    The query matches 123456 articles

Using the same Snapshot Explain object, you can also get metadata samples.

.. code-block:: python

    se.get_samples()
    print(se.samples)

.. code-block::

    TODO: Add samples response

When volume estimates are in line with your expectations, you can proceed to analyze
the data using the Snapshot TimeSeries operation, or directly extract the content via
the Snapshot Extract operation.