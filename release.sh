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
# 2. Now edit the Git SHA to tag. The version will be read from build.sbt automatically.

vers_sha=04a6fcd35aca87cf2c7bee3e790af7f3cb69f866  # the Git sha being released
vers=$(sed -n 's/^version := "\(.*\)"/\1/p' build.sbt)
if [[ -z "$vers" ]]; then
  echo "Unable to determine version from build.sbt"
  exit 1
fi

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
SKIP_REMOTE=${SKIP_REMOTE:-false}                 # set to true to skip Central/S3 uploads (dry run)
snapshot=false                                    # whether this is a SNAPSHOT release
if [[ "$vers" == *"-SNAPSHOT" ]]; then
  snapshot=true
fi

echo "Is snapshot build: ${snapshot}"

# Build and publish to the local Maven repo (used by the zipfile).
sbt clean +publishM2

# Publish to Maven Central Portal (unless dry run)
if ! $SKIP_REMOTE; then
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
fi

# Build fatjar (possibly needed for Databricks)
sbt +assembly

# Upload the docs.  We only upload 2.12, as there are no API differences in 2.13.
if ! $SKIP_REMOTE && ! $snapshot; then
  s3cmd --acl-public --no-mime-magic -M -r put target/scala-2.12/api s3://docs.couchbase.com/sdk-api/couchbase-spark-connector-$vers/
fi

# Create and upload distribution zipfiles
for scala_version in "2.12" "2.13"
do
  mkdir -p $staging_dir/$scala_version
  m2_dir=$maven_repo/com/couchbase/client/spark-connector_$scala_version/$vers
  if [[ ! -d "$m2_dir" ]]; then
    echo "Expected Maven artifacts not found: $m2_dir"
    echo "Ensure 'sbt +publishM2' completed successfully for Scala $scala_version and version $vers."
    exit 1
  fi
  cp $m2_dir/* $staging_dir/$scala_version

  # Copy the fat JAR built by sbt-assembly
  asm_jar=target/scala-$scala_version/spark-connector-assembly-$vers.jar
  if [[ ! -f "$asm_jar" ]]; then
    echo "Assembly JAR not found: $asm_jar"
    echo "Ensure 'sbt +assembly' completed successfully."
    exit 1
  fi
  cp "$asm_jar" $staging_dir/$scala_version

  pushd $staging_dir/$scala_version

  # Create and upload the zipfile.
  # We still do this for SNAPSHOT to make it easy for users to access.
  zipfile=Couchbase-Spark-Connector_$scala_version-$vers.zip
  zip $zipfile *.jar *.pom spark-connector-assembly-$vers.jar
  if ! $SKIP_REMOTE; then
    s3cmd --acl-public --no-mime-magic -M -r put $zipfile s3://packages.couchbase.com/clients/connectors/spark/$vers/
  fi

  popd
done


# Git tagging.
if ! $SKIP_REMOTE && ! $snapshot; then
  git tag -f -s -m "$vers" $vers $vers_sha
  git push gerrit $vers
fi
