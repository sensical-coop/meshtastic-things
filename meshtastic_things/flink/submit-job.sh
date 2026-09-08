#!/usr/bin/env bash
#
# Submit a PyFlink job to the cluster, detached and only if it isn't already
# running.
#
#   submit-job.sh <job-file.py> <job-name>
#
set -euo pipefail

JOB_FILE="${1:?usage: submit-job.sh <job-file.py> <job-name>}"
JOB_NAME="${2:?usage: submit-job.sh <job-file.py> <job-name>}"
REST_URL="${FLINK_REST_URL:-http://jobmanager:8081}"
WAIT_SECONDS="${FLINK_SUBMIT_WAIT_SECONDS:-180}"

deadline=$(( $(date +%s) + WAIT_SECONDS ))
until curl -sf "$REST_URL/overview" >/dev/null 2>&1; do
	if [ "$(date +%s)" -ge "$deadline" ]; then
		echo "submit-job: jobmanager REST at $REST_URL not reachable after ${WAIT_SECONDS}s" >&2
		exit 1
	fi
	sleep 2
done

# States to show that "this job exists and does not need resubmitting"
live_count() {
	curl -sf "$REST_URL/jobs/overview" 2>/dev/null | python3 -c '
import json, sys
name = sys.argv[1]
LIVE = {"RUNNING", "CREATED", "INITIALIZING", "RESTARTING", "RECONCILING", "SUSPENDED"}
try:
    jobs = json.load(sys.stdin).get("jobs", [])
except Exception:
    jobs = []
print(sum(1 for j in jobs if j.get("name") == name and j.get("state") in LIVE))
' "$JOB_NAME"
}

count="$(live_count || echo 0)"
count="${count:-0}"

if [ "$count" -gt 1 ]; then
	echo "submit-job: WARNING - $count copies of '$JOB_NAME' are already running." >&2
	echo "submit-job: cancel the extras in the Flink UI; see docs/flink.md#duplicate-jobs" >&2
	exit 0
fi

if [ "$count" -eq 1 ]; then
	echo "submit-job: '$JOB_NAME' is already running - nothing to do."
	exit 0
fi

echo "submit-job: submitting '$JOB_NAME' from $JOB_FILE (detached)"
exec flink run -d -py "$JOB_FILE"
