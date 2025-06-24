#!/bin/bash
SCRATCH_DIR=$1
SIF_PATH=$2
DOCKER_URL=$3
LOG_DIR=$4
IMAGE=$5
OVERLAY_SIZE=$6

sbatch <<EOF
#!/bin/bash
#SBATCH --job-name=analysis_build_${IMAGE}
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --mem=10G
#SBATCH --time=00:30:00
#SBATCH --output=${LOG_DIR}/build-${IMAGE}-%j.log
#SBATCH --error=${LOG_DIR}/build-${IMAGE}-%j.err

mkdir -p ${SCRATCH_DIR}/images
singularity build --force ${SIF_PATH} ${DOCKER_URL}
singularity overlay create --size ${OVERLAY_SIZE} ${SIF_PATH}
EOF