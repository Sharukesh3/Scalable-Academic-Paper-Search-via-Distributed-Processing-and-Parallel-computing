#!/bin/bash
#SBATCH --partition=workq              # Partition to submit to (Do Not Change)
#SBATCH --time=96:00:00                # Walltime (format: HH:MM:SS)
#SBATCH --gres=gpu:1                   # Request 1 (or 2) GPU if needed (this is per node)
#SBATCH --nodelist=asaicomputenode02   # Specify node(s) by name
#SBATCH --cpus-per-task=32              # Number of CPU cores per task
#SBATCH --mem=64G                      # Total memory per node
#SBATCH --output=/dist_home/suryansh/BD/dataset_cleaning/node2.log #Saving output to a log

#Load the necessary modules or environments, such as Conda
source ~/.bashrc

#Change to the directory containing your code, if necessary
cd /dist_home/suryansh/BD/dataset_cleaning

# Path to your conda installation
CONDA_PATH="$HOME/miniforge3"

# Initialize conda
source "$CONDA_PATH/etc/profile.d/conda.sh"

# Activate the environment
conda activate spark_env

#Download plz work
echo $(nproc)
nvidia-smi
SPARK_MASTER_URL="spark://asaicomputenode03.amritanet.edu:7077"
$SPARK_HOME/sbin/start-worker.sh "$SPARK_MASTER_URL"
echo "Spark Worker daemon started. Script is now waiting indefinitely."
sleep infinity
