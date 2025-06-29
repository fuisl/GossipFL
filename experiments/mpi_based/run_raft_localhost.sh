#!/bin/bash

# export entity=""  # Disable wandb by leaving empty
export entity="jv-fuisl-vietnamese-german-university"  # Your actual wandb entity
export project="gossipfl"

export cluster_name="localhost"

# Override the default mappings for single GPU with 4 workers
export GOSSIP_MPI_HOST="localhost:4"
export GOSSIP_GPU_MAPPINGS="localhost:4"

export NWORKERS=4

# Dataset and model configuration
export dataset="cifar10"
export model="cifar10flnet"

# Training configuration
export sched="StepLR"
export lr_decay_rate=0.992
export partition_method='hetero'
export partition_alpha=0.5
export lr=0.04

# Launch RAFT_GossipFL experiment
echo "Launching RAFT_GossipFL experiment with 4 workers on single GPU"
echo "Dataset: $dataset, Model: $model"
echo "Learning rate: $lr, Partition: $partition_method (alpha=$partition_alpha)"

lr=$lr algorithm="RAFT_GossipFL" bash ./launch_mpi_based.sh
