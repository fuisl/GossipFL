FROM pytorch/pytorch:2.7.1-cuda12.8-cudnn9-devel

LABEL name="GossipFL"

# Install necessary packages including sudo and passwd
RUN apt-get update && \
    apt-get install -y sudo passwd git openssh-client gnupg2 curl dos2unix && \
    apt-get clean

# Create devuser with a home dir and Bash as shell
RUN useradd -ms /bin/bash devuser && \
    echo "devuser:devuser" | chpasswd && \
    echo "devuser ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/devuser && \
    chmod 0440 /etc/sudoers.d/devuser && \
    passwd -d root

USER devuser
WORKDIR /workspaces


RUN NONINTERACTIVE=1 /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
ENV PATH="/home/linuxbrew/.linuxbrew/bin:/home/linuxbrew/.linuxbrew/sbin:$PATH"
RUN echo 'eval "$(/home/linuxbrew/.linuxbrew/bin/brew shellenv)"' >> /home/devuser/.bashrc
RUN brew install gh mpi
