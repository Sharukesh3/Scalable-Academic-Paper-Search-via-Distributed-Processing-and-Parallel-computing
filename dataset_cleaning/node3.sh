#!/bin/bash
#SBATCH --partition=workq              # Partition to submit to (Do Not Change)
#SBATCH --time=24:00:00                # Walltime (format: HH:MM:SS)
#SBATCH --gres=gpu:0                   # Request 1 (or 2) GPU if needed (this is per node)
#SBATCH --nodelist=asaicomputenode03   # Specify node(s) by name
#SBATCH --cpus-per-task=32              # Number of CPU cores per task
#SBATCH --mem=512G                      # Total memory per node
#SBATCH --output=/dist_home/suryansh/BD/dataset_cleaning/node3.log #Saving output to a log

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
SPARK_MASTER_URL="spark://asaicomputenode03.amritanet.edu:7077"
sworkers
$SPARK_HOME/sbin/start-worker.sh "$SPARK_MASTER_URL" --memory 512g
echo "Spark Worker daemon started. Script is now waiting indefinitely."
sleep infinity
