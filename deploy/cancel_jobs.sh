#!/bin/bash
SSH_USER=$1
IMAGE=$2

for jobname in "analysis_build_${IMAGE}" "analysis_run_${IMAGE}"; do
    active_jobs=$(squeue -u ${SSH_USER} -n $jobname -h -o "%i")
    if [ -n "$active_jobs" ]; then
        echo "Cancelling existing jobs for $jobname: $active_jobs"
        for jobid in $active_jobs; do
            scancel $jobid
        done
    else
        echo "No jobs to cancel for $jobname"
    fi
done