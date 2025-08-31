#!/bin/bash
#SBATCH --partition=workq              # Partition to submit to (Do Not Change)
#SBATCH --time=24:00:00                # Walltime (format: HH:MM:SS)
#SBATCH --gres=gpu:0                   # Request 1 (or 2) GPU if needed (this is per node)
#SBATCH --nodelist=asaicomputenode02   # Specify node(s) by name
#SBATCH --cpus-per-task=16              # Number of CPU cores per task
#SBATCH --mem=32G                      # Total memory per node
#SBATCH --output=/dist_home/suryansh/BD/dataset/s2orc/decompress.log #Saving output to a log

#Load the necessary modules or environments, such as Conda
source ~/.bashrc

#Change to the directory containing your code, if necessary
cd /dist_home/suryansh/BD/dataset/s2orc/s2orc_data_failed

# Path to your conda installation
CONDA_PATH="$HOME/miniforge3"

# Initialize conda
source "$CONDA_PATH/etc/profile.d/conda.sh"

# Activate the environment
conda activate base

#Decompressing
pigz -dc -p $(nproc) 20250815_113147_00039_6wjwx_795a8455-11d8-46de-a5c8-7c96bf6c3c98.gz > /dist_home/suryansh/BD/dataset/s2orc/s2orc_data_ectracted/20250815_113147_00039_6wjwx_795a8455-11d8-46de-a5c8-7c96bf6c3c98.json
pigz -dc -p $(nproc) 20250815_113147_00039_6wjwx_d9eb6856-ae70-4c7a-8610-0e70e151cbd6.gz > /dist_home/suryansh/BD/dataset/s2orc/s2orc_data_ectracted/20250815_113147_00039_6wjwx_d9eb6856-ae70-4c7a-8610-0e70e151cbd6.json

#Moving all gz to where they belong
mv *.gz ../s2orc_data/
cd ..
rmdir s2orc_data_failed
