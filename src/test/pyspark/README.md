Examples and tests using the Couchbase Spark Connector with PySpark.

Requirements:
* A running Couchbase cluster.
* With the `travel-sample` sample bucket loaded.

To get started:
```
# Create a virtual environment (not essential but a good standard Python best practice)
python -m venv venv

# Windows
.\venv\Scripts\activate

# Linux and MacOS
source ./venv/bin/activate

# Install the requirements
pip install -r requirements.txt

cp .env.template .env
```

Now edit the `.env` file to point at your cluster.

Running the tests:
```
pytest
```