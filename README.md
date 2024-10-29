# Blockchain Insights COMAI Indexer

## Requirements
- Docker Compose  
- Rust (latest stable)
- Node, NPM (LTS)

The following guides are for Ubuntu 22, but MacOS and Windows can also be used and should be compatible.

- Docker:
Follow the guide for your OS using the instructions in
[this guide](https://docs.docker.com/desktop/). After this, install the Compose plugin. The following guide is for Ubuntu.

    ```bash
    # if you don't have the repo/certificates installed in 
    sudo apt-get update
    sudo apt-get install ca-certificates curl
    sudo install -m 0755 -d /etc/apt/keyrings
    sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
    sudo chmod a+r /etc/apt/keyrings/docker.asc
    
    # Add the repository to Apt sources:
    echo \
    "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
    $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
    sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
    sudo apt-get update
    ```
  
- Rust:
  - For Linux, run `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
  - Otherwise, follow the installation guide in [rustup.rs](https://rustup.rs/#).

- Node.js:
    ```bash
    # installs nvm (Node Version Manager)
    curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.0/install.sh | bash
    # download and install Node.js (you may need to restart the terminal)
    nvm install 20
    # verifies the right Node.js version is in the environment
    node -v # should print `v20.18.0`
    # verifies the right npm version is in the environment
    npm -v # should print `10.8.2`
    ```

## Components
### Subquery Indexers (subql-balance-tracking,  subql-funds-flow)
Either funds-flow or balance-tracking can be run in the same manner. Postgres ports for balance-tracking are different than in funds-flow for the subql components. 

The `balance-tracking` subql indexer is stand alone and does not require another component, while the `funds-flow` indexer requires **both** the subql component and rs-indexer.


```bash
cd subql-funds-flow
npm i -g @subql/cli@5.2.8
npm install
npm run build
npm run start:docker # if docker compose v1
# or
docker compose pull # if docker compose v2
docker compose up
```

### Rust Indexer (rs-indexer)
The Rust indexer is to create the funds flow model. It will require you to be running the subql-funds-flow docker images.

#### Requirements
- Neo4j Database: There is a docker-compose.yml file that provides settings for a local Neo4j database. It can be run using `docker compose up neo4j`.
- subql-funds-flow indexer

Set `NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD`, `SUBQL_URL` variables in a .env file according to the `.env.example` file provided, so copy that file and set the values as necessary.

```bash
cp .env.example .env
```

`SUBQL_URL` is where the subquery indexer endpoints are, which by default is `http://localhost:3000`

```bash
cd rs-indexer
cargo run
```





