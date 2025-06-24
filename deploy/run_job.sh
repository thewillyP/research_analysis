#!/bin/bash
LOG_DIR=$1
SIF_PATH=$2
SSH_USER=$3
VARIANT=$4
BUILD_JOB_ID=$5
DB_HOST=$6
POSTGRES_USER=$7
POSTGRES_PASSWORD=$8
POSTGRES_DB=$9
PGPORT=${10}
IMAGE=${11}
TMP_DIR=${12}
JOB_RUNTIME=${13}
GPU_COUNT=${14}
MEMORY=${15}
CPU_COUNT=${16}
PYTHON_TASKS=${17}

# Set GPU-specific options based on VARIANT
if [ "$VARIANT" = "gpu" ]; then
    GPU_SLURM="#SBATCH --gres=gpu:${GPU_COUNT}"
    GPU_SINGULARITY="--nv"
else
    GPU_SLURM=""
    GPU_SINGULARITY=""
fi

# Set dependency if BUILD_JOB_ID is provided
if [ -n "$BUILD_JOB_ID" ]; then
    SLURM_DEPENDENCY="#SBATCH --dependency=afterok:$BUILD_JOB_ID"
else
    SLURM_DEPENDENCY=""
fi

# Create temporary script to run all Python tasks
TASK_SCRIPT="${TMP_DIR}/run_tasks_${IMAGE}.sh"
echo "#!/bin/bash" > "${TASK_SCRIPT}"
IFS=';' read -ra TASKS <<< "${PYTHON_TASKS}"
for task in "${TASKS[@]}"; do
    # Split task into file path and arguments
    read -ra TASK_ARGS <<< "${task}"
    PYTHON_FILE="${TASK_ARGS[0]}"
    ARGS="${TASK_ARGS[@]:1}"
    echo "echo 'Running ${PYTHON_FILE} with arguments: ${ARGS}'" >> "${TASK_SCRIPT}"
    echo "python3 ${PYTHON_FILE} ${ARGS}" >> "${TASK_SCRIPT}"
done
chmod +x "${TASK_SCRIPT}"

# Submit SLURM job
sbatch <<EOF
#!/bin/bash
#SBATCH --job-name=analysis_run_${IMAGE}
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --mem=${MEMORY}
#SBATCH --time=${JOB_RUNTIME}
#SBATCH --cpus-per-task=${CPU_COUNT}
#SBATCH --output=${LOG_DIR}/task-${IMAGE}-%j.log
#SBATCH --error=${LOG_DIR}/task-${IMAGE}-%j.err
${GPU_SLURM}
${SLURM_DEPENDENCY}

singularity exec ${GPU_SINGULARITY} \\
  --containall --no-home --cleanenv \\
  --bind /home/${SSH_USER}/.ssh \\
  --bind /home/${SSH_USER}/dev \\
  --bind /scratch/${SSH_USER}/wandb:/wandb_data \\
  --bind /scratch/${SSH_USER}/space:/scratch \\
  --bind ${TMP_DIR}:/tmp \\
  ${DB_HOST:+--env DB_HOST=${DB_HOST}} \\
  ${POSTGRES_USER:+--env POSTGRES_USER="${POSTGRES_USER}"} \\
  ${POSTGRES_PASSWORD:+--env POSTGRES_PASSWORD="${POSTGRES_PASSWORD}"} \\
  ${POSTGRES_DB:+--env POSTGRES_DB=${POSTGRES_DB}} \\
  ${PGPORT:+--env PGPORT=${PGPORT}} \\
  ${SIF_PATH} \\
  /bin/bash ${TASK_SCRIPT}
EOF