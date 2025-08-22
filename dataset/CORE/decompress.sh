#!/bin/bash
#SBATCH --partition=workq              # Partition to submit to (Do Not Change)
#SBATCH --time=96:00:00                # Walltime (format: HH:MM:SS)
#SBATCH --gres=gpu:0                   # Request 1 (or 2) GPU if needed (this is per node)
#SBATCH --nodelist=asaicomputenode03   # Specify node(s) by name
#SBATCH --cpus-per-task=16              # Number of CPU cores per task
#SBATCH --mem=64G                      # Total memory per node
#SBATCH --output=/dist_home/suryansh/BD/dataset/CORE/decompress.log #Saving output to a log

#Load the necessary modules or environments, such as Conda
source ~/.bashrc

#Change to the directory containing your code, if necessary
cd /dist_home/suryansh/BD/dataset/CORE

# Path to your conda installation
CONDA_PATH="$HOME/miniforge3"

# Initialize conda
source "$CONDA_PATH/etc/profile.d/conda.sh"

# Activate the environment
conda activate base

#Download plz work
echo $(nproc)
pigz --version
pigz -d -p $(nproc) core_2018-03-01_fulltext.tar.gz
echo "decompression of gz done"
ionice -c2 -n0 tar xf core_2018-03-01_fulltext.tar -C core_2018_fulltext_extracted
echo "decompression of .tar done"
