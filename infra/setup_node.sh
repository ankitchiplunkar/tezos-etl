# sudo bash

function update_system() {
    apt-get update
    apt-get install \
    apt-transport-https \
    ca-certificates \
    curl \
    software-properties-common -y
}

function install_docker() {
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -

    add-apt-repository \
    "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
    $(lsb_release -cs) \
    stable"

    apt-get update
    apt-get install docker-ce -y
    apt-get install ctop -y
    systemctl enable docker
}

function install_docker_compose() {
    sudo curl -L "https://github.com/docker/compose/releases/download/1.25.4/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
    sudo chmod +x /usr/local/bin/docker-compose
}

update_system
install_docker
install_docker_compose

wget -O mainnet.sh https://gitlab.com/tezos/tezos/raw/master/scripts/alphanet.sh
chmod +x mainnet.sh

./mainnet.sh start --rpc-port 127.0.0.1:8732