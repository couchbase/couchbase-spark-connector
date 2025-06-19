# Couchbase Spark Connector release script
# ========================================
#
# For internal use by Couchbase developers.
#
# Snapshot handling: snapshot builds are built locally, but not published to Sonatype, or Git-tagged.
#
# 1. Before running this script:
#  * Bump the version numbers in build.sbt and README.md.
#  * Push to Gerrit and get a review.
#  * Pull the changes again.
#
# 2. Now edit these versions:

vers_sha=04a6fcd35aca87cf2c7bee3e790af7f3cb69f866  # the Git sha being released
vers=3.5.0-SNAPSHOT                                         # the version being released

# 3. Now run the script from the script's directory, e.g.
#   cd couchbase-spark-connector
#   chmod +x release.sh
#   ./release.sh
#
# 4. After the script finishes successfully:
#  * Bump the version numbers in build.sbt for next SNAPSHOT.
#  * Push all changes to Gerrit for review, including changes to this file.
#  * Release on Jira.
#  * Release notes.
#  * Release the uploaded bundle via https://central.sonatype.com/publishing/deployments

set -e
set -x

# Variables.
staging_dir=~/temp/staging/couchbase-spark-connector/$vers
maven_repo=~/.m2/repository
snapshot=false                                    # whether this is a SNAPSHOT release
if [[ "$vers" == *"-SNAPSHOT" ]]; then
  snapshot=true
fi

echo "Is snapshot build: ${snapshot}"

# Git tagging.
if ! $snapshot; then
  git tag -f -s -m "$vers" $vers $vers_sha
  git push gerrit $vers
fi

# Build and publish to the local Maven repo (used by the zipfile).
sbt clean +publishM2

# Build fatjar (needed for Databricks).
sbt +assembly

# Publish to Maven Central Portal
if $snapshot; then
  # Publish snapshots directly
  sbt clean +publishSigned
else
  # Creates a local staging dir
  sbt clean +publishSigned
  # Uploads the staging dir to Central Portal
  sbt sonatypeCentralUpload
  # Release via https://central.sonatype.com/publishing/deployments
fi

# Upload the docs.  We only upload 2.12, as there are no API differences in 2.13.
if ! $snapshot; then
  s3cmd --acl-public --no-mime-magic -M -r put target/scala-2.12/api s3://docs.couchbase.com/sdk-api/couchbase-spark-connector-$vers/
fi

# Create and upload distribution zipfiles
for scala_version in "2.12" "2.13"
do
  mkdir -p $staging_dir/$scala_version
  cp $maven_repo/com/couchbase/client/spark-connector_$scala_version/$vers/* $staging_dir/$scala_version
  cp target/scala-$scala_version/spark-connector-assembly-$vers.jar $staging_dir/$scala_version

  pushd $staging_dir/$scala_version

  # Create and upload the zipfile.
  # We still do this for SNAPSHOT to make it easy for users to access.
  zipfile=Couchbase-Spark-Connector_$scala_version-$vers.zip
  zip $zipfile *.jar *.pom spark-connector-assembly-$scala_version-$vers.jar
  s3cmd --acl-public --no-mime-magic -M -r put $zipfile s3://packages.couchbase.com/clients/connectors/spark/$vers/

  popd
done