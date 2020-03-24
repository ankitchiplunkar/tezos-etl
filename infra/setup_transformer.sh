function update_system() {
    apt-get update
    apt-get install \
    apt-transport-https \
    ca-certificates \
    curl \
    software-properties-common -y
}

function setup_python3() {
    sudo apt-get install virtualenv -y
    sudo apt-get install python3.6-dev -y
    sudo apt-get install python3 python-dev python3-dev \
        build-essential libssl-dev libffi-dev \
        libxml2-dev libxslt1-dev zlib1g-dev \
        python-pip python3-pip -y
}

function setup_tezos_etl() {
    cd tezos-query/tezos_etl
    virtualenv -p python3 venv
    source venv/bin/activate
    pip3 install -r requirements.txt
    pip3 install .
    deactivate
    mkdir ../raw-data ../avro-data
    cd
}

update_system
setup_python3
git clone https://github.com/ankitchiplunkar/tezos-query.git
# perform auth
setup_tezos_etl