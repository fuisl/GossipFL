#!/usr/bin/bash

echo module load mpi/openmpi-x86_64 >> ~/.bashrc

# git clone the repo
# git clone https://github.com/fuisl/GossipFL.git

# setup environment
cd ~
python3.9 -m venv .gossipfl
source ~/.gossipfl/bin/activate
pip install yacs fedml[MPI]

# cd into the repo
# cd GossipFL
