# Examples and tests using the Couchbase Spark Connector with PySpark.

## Requirements
The only requirement is a running Couchbase cluster.  

If you don't already have one, a great option is to use the [Couchbase Capella free tier](https://docs.couchbase.com/cloud/get-started/create-account.html).

Any sort of cluster is supported including Columnar, but of course make sure you are running a test, notebook or example that is compatible with that cluster type.

A running Spark cluster is _not_ required, as all examples will use `.master("local[*]")` by default - but this can be overridden in your `.env` file below to point at your actual Spark cluster, if you are using one.

## Get started
If you don't have the project cloned already:
```
git clone https://github.com/couchbase/couchbase-spark-connector/
cd couchbase-spark-connector/src/test/pyspark
```

Download the connector (see [here](https://docs.couchbase.com/spark-connector/current/download-links.html) for the latest version):
```
wget https://packages.couchbase.com/clients/connectors/spark/3.5.2/Couchbase-Spark-Connector_2.12-3.5.2.zip
unzip Couchbase-Spark-Connector_2.12-3.5.2.zip -d connector
```

Then create a virtual environment (not essential but a good standard Python best practice):

```
python -m venv venv

# Windows
.\venv\Scripts\activate

# Linux and MacOS
source ./venv/bin/activate

# Install the requirements
python -m pip install -r requirements.txt
```
(Of course, if you prefer Conda or similar, please feel free to use instead.)

Now create the `.env` file:

```
cp .env.template .env
```

And edit the `.env` file to point at your Couchbase cluster.

## Running Jupyter notebooks
Running a Jupyter notebook:
```
jupyter notebook ./examples/basic/pyspark_jupyter_notebook_example.ipynb
```

Jupyter can also be run from an IDE and from 3rd-party providers such as Databricks and AWS EMR.

## Running tests
Note that tests require the `travel-sample` sample bucket to be loaded.

Running the tests:
```
pytest
```

(Set `COUCHBASE_SSL_INSECURE=true` in `.env` only for local development to disable SSL verification. Do not use in production.)