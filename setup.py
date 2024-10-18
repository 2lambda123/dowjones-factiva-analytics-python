from setuptools import setup, find_packages

with open("README.rst", "r") as fh:
    long_desc = fh.read()

with open('src/factiva/analytics/__version__.py') as f:
    for line in f:
        if line.find("__version__") >= 0:
            version = line.split("=")[1].strip()
            version = version.strip('"')
            version = version.strip("'")
            continue

setup(
    name='factiva-analytics',
    version=version,
    description='Python package to interact with Factiva Analytics APIs. Services are described in the Dow Jones Developer Platform.',
    long_description=long_desc,
    long_description_content_type='text/x-rst',
    license='MIT',
    author='Dow Jones Customer Engineers',
    author_email='customer.solutions@dowjones.com',

    # Warning: the folder 'factiva' should NOT have an __init__.py file to avoid conflicts with the same namespace across other packages
    package_dir={'': 'src'},
    # packages=['factiva.analytics', 'factiva.analytics.article_retrieval', 'factiva.analytics.auth',
    #           'factiva.analytics.common', 'factiva.analytics.taxonomy', 'factiva.analytics.snapshots',
    #           'factiva.analytics.streams', 'factiva.analytics.integration'
    #         #   , 'factiva.analytics.lists',
    #           ],
    packages=find_packages(where='src'),
    url='https://developer.dowjones.com/',
    project_urls={
            "GitHub": "https://github.com/dowjones/factiva-analytics-python",
            "Documentation": "https://factiva-analytics-python.readthedocs.io/",
            "Bug Tracker": "https://github.com/dowjones/factiva-analytics-python/issues",
        },

    classifiers=[  # Optional
        # How mature is this project? Common values are
        #   1 - Planning
        #   2 - Pre-Alpha
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 4 - Beta',

        # Indicate who your project is intended for
        'Intended Audience :: Science/Research',
        'Intended Audience :: Financial and Insurance Industry',
        'Intended Audience :: Healthcare Industry',
        'Operating System :: OS Independent',
        'Topic :: Office/Business :: News/Diary',
        'Topic :: Office/Business :: Financial :: Investment',
        'Topic :: Office/Business :: Financial :: News',
        'Topic :: Office/Business :: Financial :: Risk Management',
        'Topic :: Office/Business :: Financial :: Compliance',
        'Topic :: Office/Business :: Financial :: Trading',
        'Topic :: General :: News',
        'Topic :: General :: Financial',
        'Topic :: General :: Investment',
        'Topic :: General :: Artificial Intelligence',
        'Topic :: Scientific/Engineering :: Artificial Intelligence',

        'License :: OSI Approved :: MIT License',

        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Programming Language :: Python :: 3.13'
    ],
    keywords='news, news aggregator, risk, compliance, nlp, alternative data, factiva, trading news, market movers',
    # Required version conditioned by typed lists and Pandas
    python_requires='>=3.10.0',
    install_requires=[
        'requests>=2.30.0',
        'pandas>=2.2.0',
        'fastavro>=1.9.0',
        'google-cloud-core>=2.4.0',
        'google-cloud-pubsub>=2.26.0'
    ],
    extras_require={
        'dev': [
            'pytest>=6.0',
            'sphinx>=3.0',
        ],
        'mongodb': ['pymongo'],
        'elasticsearch': ['elasticsearch'],
        'bigquery': ['google-cloud-bigquery']
    })
