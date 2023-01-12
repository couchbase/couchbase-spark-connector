# Couchbase Spark Connector release script
# ========================================
#
# For internal use by Couchbase developers
#
# 1. Before running this script:
#  * Bump the version numbers in build.sbt and README.md.
#  * Push to Gerrit and get a review.
#  * Pull the changes again.
#
# 2. Now edit these versions:

vers_sha=10afea7ecc468ad810bc1475081a8f0585027533  # the Git sha being released
vers=3.3.0                                         # the version being released

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
#  * Release the staged Sonatype repo.


set -e
set -x

# Variables.
staging_dir=~/temp/staging/couchbase-spark-connector/$vers
maven_repo=~/.m2/repository
name=spark-connector_2.12-$vers

# Git tagging.
git tag -f -s -m "$vers" $vers $vers_sha
git push gerrit $vers

# Build and publish to the local Maven repo.
sbt clean publishM2

# Build fatjar (needed for Databricks).
sbt assembly

# Upload the docs.
s3cmd --acl-public --no-mime-magic -M -r put target/scala-2.12/api s3://docs.couchbase.com/sdk-api/couchbase-spark-connector-$vers/

# Stage to Sonatype.  Not allowed to upload from local repo so need to copy to a staging dir.
mkdir -p $staging_dir
cp $maven_repo/com/couchbase/client/spark-connector_2.12/$vers/* $staging_dir
cp target/scala-2.12/spark-connector-assembly-$vers.jar $staging_dir
pushd $staging_dir
mvn gpg:sign-and-deploy-file -Durl=https://oss.sonatype.org/service/local/staging/deploy/maven2/ -DrepositoryId=ossrh -DpomFile=$name.pom -Dfile=$name.jar -Dpackaging=jar
mvn gpg:sign-and-deploy-file -Durl=https://oss.sonatype.org/service/local/staging/deploy/maven2/ -DrepositoryId=ossrh -DpomFile=$name.pom -Dfile=$name-javadoc.jar -Dclassifier=javadoc -Dpackaging=jar
mvn gpg:sign-and-deploy-file -Durl=https://oss.sonatype.org/service/local/staging/deploy/maven2/ -DrepositoryId=ossrh -DpomFile=$name.pom -Dfile=$name-sources.jar -Dclassifier=sources -Dpackaging=jar

# Create and upload the zipfile.
zipfile=Couchbase-Spark-Connector_2.12-$vers.zip
zip $zipfile *.jar *.pom spark-connector-assembly-$vers.jar
s3cmd --acl-public --no-mime-magic -M -r put $zipfile s3://packages.couchbase.com/clients/connectors/spark/$vers/

popd
