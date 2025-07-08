FROM quay.io/fedora/fedora-minimal:42

# No pulling optional dependencies
RUN echo install_weak_deps=False >> /etc/dnf/dnf.conf && \
    dnf install -y git sudo openssh-server nano wget tar bash-completion python3.9 openmpi-devel --skip-unavailable && \
    dnf clean all && rm -rf /var/cache/dnf

# Create a user for ssh
RUN useradd -m dev && \
# Passwordless sudo privilege
    echo "dev ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers.d/dev && \
    chmod 0440 /etc/sudoers.d/dev

# uncomment when deploying outside of github codespace

# switch to dev user
# USER dev
# RUN setup.sh