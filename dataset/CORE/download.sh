#!/bin/bash
#SBATCH --partition=workq              # Partition to submit to (Do Not Change)
#SBATCH --time=96:00:00                # Walltime (format: HH:MM:SS)
#SBATCH --gres=gpu:0                   # Request 1 (or 2) GPU if needed (this is per node)
#SBATCH --nodelist=asaicomputenode03   # Specify node(s) by name
#SBATCH --cpus-per-task=16             # Number of CPU cores per task
#SBATCH --mem=64G                      # Total memory per node
#SBATCH --output=/dist_home/suryansh/BD/dataset/CORE/download.log #Saving output to a log

#Load the necessary modules or environments, such as Conda
source ~/.bashrc

#Change to the directory containing your code, if necessary
cd /dist_home/suryansh/BD/dataset/CORE

#Download plz work
wget --user-agent="Mozilla/5.0" https://core.ac.uk/datasets/core_2018-03-01_fulltext.tar.gz
