#!/bin/bash
#SBATCH --partition=workq              # Partition to submit to (Do Not Change)
#SBATCH --time=24:00:00                # Walltime (format: HH:MM:SS)
#SBATCH --gres=gpu:0                   # Request 1 (or 2) GPU if needed (this is per node)
#SBATCH --nodelist=asaicomputenode02   # Specify node(s) by name
#SBATCH --cpus-per-task=8              # Number of CPU cores per task
#SBATCH --mem=16G                      # Total memory per node
#SBATCH --output=/dist_home/suryansh/BD/dataset/s2orc/load_hdfs.log #Saving output to a log

#Load the necessary modules or environments, such as Conda
source ~/.bashrc

#Change to the directory containing your code, if necessary
cd /dist_home/suryansh/BD/dataset/s2orc/s2orc_data_ectracted

# Path to your conda installation
CONDA_PATH="$HOME/miniforge3"

# Initialize conda
source "$CONDA_PATH/etc/profile.d/conda.sh"

# Activate the environment
conda activate base

#Download plz work
echo $(nproc)
xargs --version

LOGFILE="uploaded_files.log"
TARGET_DIR="/s2orc_s2orc_data_extracted"

# export logfile so parallel jobs can append safely
touch "$LOGFILE"

# find files < 1GB and upload if not already logged
find . -type f -name "*.json" | while read -r file; do
    if ! grep -Fxq "$file" "$LOGFILE"; then
        echo "Uploading $file ..."
        if hdfs dfs -put "$file" "$TARGET_DIR"/; then
            echo "$file" >> "$LOGFILE"
        else
            echo "❌ Failed: $file"
        fi
    else
        echo "Skipping (already uploaded): $file"
    fi
done
