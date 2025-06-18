#!/bin/bash

# Total number of runs to generate
TOTAL_RUNS=100000
RUNS_PER_JOB=25000

# Calculate number of jobs needed
NUM_JOBS=$(( (TOTAL_RUNS + RUNS_PER_JOB - 1) / RUNS_PER_JOB ))

echo "Submitting $NUM_JOBS jobs to generate $TOTAL_RUNS runs in chunks of $RUNS_PER_JOB"

for i in $(seq 2 $NUM_JOBS); do
    # Calculate start and end run numbers for this job
    START_FROM=$(( (i-1) * RUNS_PER_JOB + 1 ))
    END_AT=$(( i * RUNS_PER_JOB ))
    
    # Ensure the last job doesn't exceed TOTAL_RUNS
    if [ $END_AT -gt $TOTAL_RUNS ]; then
        END_AT=$TOTAL_RUNS
    fi
    
    echo "Submitting job $i: runs $START_FROM to $END_AT"
    
    # Create pod manifest for this job
    cat scripts/gen-long-pod.yaml | \
        sed "s/\${JOB_SUFFIX}/$i/g" | \
        sed "s/\${START_FROM}/$START_FROM/g" | \
        sed "s/\${END_AT}/$END_AT/g" > "scripts/gen-long-pod-4cpus-job$i.yaml"
    
    # Submit the pod
    kubectl apply -f "scripts/gen-long-pod-4cpus-job$i.yaml"
    
    # Clean up the temporary manifest
    rm "scripts/gen-long-pod-4cpus-job$i.yaml"
done

echo "All jobs submitted. Monitor progress with: kubectl get pods -n vlad-dev | grep data-generation-pod" 