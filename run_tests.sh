#!/usr/bin/env bash
#
# Release gate: tests the *published* connector against every supported Spark runtime.
#
# For each runtime, `testPublished` publishes the connector to ~/.m2 - built against the release
# Spark profile - and then runs the full suite against it with that runtime's Spark on the
# classpath.
#
# Usage:
#   ./run_tests.sh              # every runtime in the support window
#   ./run_tests.sh 4.2          # only the given runtime(s)

set -uo pipefail
cd "$(dirname "$0")"

RESULTS_DIR=results
mkdir -p "$RESULTS_DIR"

# Protected against concurrent sbt runs which cause confusing error messages.
if pgrep -f "java .*sbt-launch\.jar" >/dev/null 2>&1; then
  echo "ERROR: an sbt process is already running against this repo. Wait for it to finish." >&2
  exit 1
fi

if [ $# -gt 0 ]; then
  runtimes=("$@")
else
  mapfile -t runtimes < <(./sbtx -batch -error sparkProfiles)
fi

if [ ${#runtimes[@]} -eq 0 ]; then
  echo "No Spark profiles found - is the sparkProfiles task still defined in build.sbt?" >&2
  exit 1
fi

echo "Building the connector into mavenLocal (so it's the connector that will be published and used by end-users), and testing that same build against different Spark runtimes: ${runtimes[*]}"
echo

declare -A status
overall=0

for runtime in "${runtimes[@]}"; do
  log="$RESULTS_DIR/published_runtime_${runtime//./_}.log"
  echo "=== runtime $runtime -> $log ==="

  if ./sbtx -batch "-Dspark.runtime=$runtime" testPublished > "$log"; then
    status[$runtime]=PASS
  else
    status[$runtime]=FAIL
    overall=1
  fi
  echo
done

echo "================ summary ================"
for runtime in "${runtimes[@]}"; do
  log="$RESULTS_DIR/published_runtime_${runtime//./_}.log"
  tally=$(grep -oE "Total [0-9]+, Failed [0-9]+, Errors [0-9]+, Passed [0-9]+(, Skipped [0-9]+)?" \
    "$log" | tail -1)
  printf '%-6s %-4s %s\n' "$runtime" "${status[$runtime]}" "$tally"
  if [ "${status[$runtime]}" = "FAIL" ]; then
    grep -oE "Test [a-zA-Z0-9.]+ failed: [^,]{0,90}" "$log" | sort -u | head -5 | sed 's/^/       /'
  fi
done
echo "========================================="

exit $overall
