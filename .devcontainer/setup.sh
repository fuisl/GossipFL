#!/usr/bin/bash

cd ~

echo module load mpi/openmpi-x86_64 >> .bashrc

# git clone the repo
# mkdir GossipFL
# cd GossipFL
# git clone https://github.com/fuisl/GossipFL.git
# cd ~

# setup environment
python3.9 -m venv .gossipfl
source ~/.gossipfl/bin/activate
pip install grpcio protobuf numpy paho-mqtt pyyaml torch pandas wandb yacs h5py torchvision tqdm

# install requirements
# cd GossipFL
# pip install -r requirement.txt
