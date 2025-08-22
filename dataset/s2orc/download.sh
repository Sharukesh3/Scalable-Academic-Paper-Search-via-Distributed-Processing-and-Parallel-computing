#!/bin/bash
#SBATCH --partition=workq              # Partition to submit to (Do Not Change)
#SBATCH --time=96:00:00                # Walltime (format: HH:MM:SS)
#SBATCH --gres=gpu:0                   # Request 1 (or 2) GPU if needed (this is per node)
#SBATCH --nodelist=asaicomputenode03   # Specify node(s) by name
#SBATCH --cpus-per-task=8              # Number of CPU cores per task
#SBATCH --mem=16G                      # Total memory per node
#SBATCH --output=/dist_home/suryansh/BD/dataset/s2orc/download.log #Saving output to a log

#Load the necessary modules or environments, such as Conda
source ~/.bashrc

#Change to the directory containing your code, if necessary
cd /dist_home/suryansh/BD/dataset/s2orc

#Download plz work
python api.py
