# Blockchain Insights COMAI Indexer

## Requirements
- Docker Compose
- Rust (latest stable)
- Node, NPM (LTS)

## Components
### Subquery Indexer (Subql)
```bash
cd subql
npm install
npm run build
npm run start:docker # if docker compose v1
# or
docker compose pull # if docker compose v2
docker compose up
```

### Rust Indexer (rs-indexer)
#### Requirements
- Neo4j Database

Set `NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD`, `SUBQL_URL` variables in a .env file according to the `.env.example` file provided.

`SUBQL_URL` is where the subquery indexer endpoints are, which by default is `http://localhost:3000`

```bash
cd rs-indexer
cargo run
```





