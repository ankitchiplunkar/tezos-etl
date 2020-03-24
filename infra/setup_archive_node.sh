# sudo bash

function update_system() {
    apt-get update
    
    apt-get install \
    apt-transport-https \
    ca-certificates \
    curl \
    software-properties-common -y
    
    apt-get install build-essential \
    git m4 unzip rsync curl bubblewrap \
    libev-dev libgmp-dev pkg-config \
    libhidapi-dev jbuilder -y
}

function install_opam() {
    add-apt-repository ppa:avsm/ppa
    apt update
    apt install opam -y
    opam init
    opam update
    eval $(opam env)
}

function install_tezos() {
    git clone https://gitlab.com/tezos/tezos.git
    cd tezos
    git checkout mainnet
    git rev-parse HEAD
    make build-deps 
    eval $(opam env)
    make
}

function run_tezos() {
    ./tezos-node identity generate
    nohup ./tezos-node run --rpc-addr :8732 --connections 20 \
    --history-mode archive --log-output tezos.log &
}

update_system
install_opam
install_tezos