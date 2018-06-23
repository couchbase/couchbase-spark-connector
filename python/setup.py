from setuptools import setup

VERSION = "2.2.0"


setup(
    name='pyspark_couchbase',
    version=VERSION,
    url="https://github.com/couchbase/couchbase-spark-connector",
    license="Apache License 2.0",
    description="Utilities for using pyspark with couchbase",
    long_description=open("README.rst", "r").read(),
    keywords=["couchbase", "nosql", "pyspark"],
    classifiers=[
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 3",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Python Modules"],
    packages=[
        'pyspark_couchbase'],
    install_requires=[],
    tests_require=['nose'],
    test_suite='tests',
    zip_safe=True
)
