#!/bin/bash
#SBATCH --partition=workq              # Partition to submit to (Do Not Change)
#SBATCH --time=96:00:00                # Walltime (format: HH:MM:SS)
#SBATCH --gres=gpu:0                   # Request 1 (or 2) GPU if needed (this is per node)
#SBATCH --nodelist=asaicomputenode02   # Specify node(s) by name
#SBATCH --cpus-per-task=48              # Number of CPU cores per task
#SBATCH --mem=96G                      # Total memory per node
#SBATCH --output=/dist_home/suryansh/BD/dataset/s2orc/decompress.log #Saving output to a log

#Load the necessary modules or environments, such as Conda
source ~/.bashrc

#Change to the directory containing your code, if necessary
cd /dist_home/suryansh/BD/dataset/s2orc/paper_data

# Path to your conda installation
CONDA_PATH="$HOME/miniforge3"

# Initialize conda
source "$CONDA_PATH/etc/profile.d/conda.sh"

# Activate the environment
conda activate base

#Decompressing
for f in /dist_home/suryansh/BD/dataset/s2orc/paper_data/*.gz; do
    pigz -dc -p $(nproc) "$f" > /dist_home/suryansh/BD/dataset/s2orc/paper_data_ectracted/$(basename "${f%.gz}")
done
