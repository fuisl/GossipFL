#!/bin/bash

# export entity=""  # Disable wandb by leaving empty
export entity="jv-fuisl-vietnamese-german-university"  # Your actual wandb entity
export project="gossipfl"

cluster_name="localhost"

# Dataset and model configuration
dataset="mnist"
model="mnistflnet"
algorithm="RAFT_GossipFL"

# Training configuration
sched="StepLR"
lr_decay_rate=0.992
partition_method='hetero'
partition_alpha=0.5
lr=0.04

dir_name=$PWD

if [ "$dataset" == "Tiny-ImageNet-200" ]; then
    data_dir="${data_dir:-$dir_name/datasets/tiny-imagenet-200}"
elif [ "$dataset" == "cifar10" ]; then
    data_dir="${data_dir:-$dir_name/datasets/cifar10}"
elif [ "$dataset" == "cifar100" ]; then
    data_dir="${data_dir:-$dir_name/datasets/cifar100}"
elif [ "$dataset" == "fmnist" ]; then
    data_dir="${data_dir:-$dir_name/datasets/fmnist}"
elif [ "$dataset" == "SVHN" ]; then
    data_dir="${data_dir:-$dir_name/datasets/SVHN}"
elif [ "$dataset" == "mnist" ]; then
    data_dir="${data_dir:-$dir_name/datasets}"
fi


source ${dir_name}/experiments/configs_system/$cluster_name.conf
source ${dir_name}/experiments/configs_model/$model.conf
source ${dir_name}/experiments/configs_algorithm/$algorithm.conf
source ${dir_name}/experiments/main_args.conf

main_args="${main_args:-  }"

PYTHON="${PYTHON:-python}"

export WANDB_CONSOLE=off

$PYTHON ./standalone_raft_node.py "$@" $main_args 
