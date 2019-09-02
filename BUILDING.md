# Building the Couchbase Spark Connector

## Building docs (for testing)
Install Antora 2, then
```
antora staging-antora-playbook.yml
```

The docs are created in `docs\public`.

## Building
```
./gradlew assemble

```
Files are in `build/libs`.  You should have a jar, plus source and scaladoc jars.

```
./gradlew shadowJar
```

Produces the 'fatjar' containing the Spark connector along with all its recursive dependencies, suitable for use with e.g. `spark-shell`.