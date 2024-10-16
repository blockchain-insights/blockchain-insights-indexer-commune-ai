# SubQuery - Example Project for Bittensor

[SubQuery](https://subquery.network) is a fast, flexible, and reliable open-source data indexer that provides you with custom APIs for your web3 project across all of our supported networks. To learn about how to get started with SubQuery, [visit our docs](https://academy.subquery.network).

**This SubQuery project indexes all asset transfers using the balances pallet on the Bittensor Network**

## Start

First, install SubQuery CLI globally on your terminal by using NPM `npm install -g @subql/cli`

You can either clone this GitHub repo, or use the `subql` CLI to bootstrap a clean project in the network of your choosing by running `subql init` and following the prompts.

Don't forget to install dependencies with `npm install` or `yarn install`!

## Editing your SubQuery project

Although this is a working example SubQuery project, you can edit the SubQuery project by changing the following files:

- The project manifest in `project.yaml` defines the key project configuration and mapping handler filters
- The GraphQL Schema (`schema.graphql`) defines the shape of the resulting data that you are using SubQuery to index
- The Mapping functions in `src/mappings/` directory are typescript functions that handle transformation logic

SubQuery supports various layer-1 blockchain networks and provides [dedicated quick start guides](https://academy.subquery.network/quickstart/quickstart.html) as well as [detailed technical documentation](https://academy.subquery.network/build/introduction.html) for each of them.

## Run your project

_If you get stuck, find out how to get help below._

The simplest way to run your project is by running `yarn dev` or `npm run-script dev`. This does all of the following:

1.  `yarn codegen` - Generates types from the GraphQL schema definition and contract ABIs and saves them in the `/src/types` directory. This must be done after each change to the `schema.graphql` file or the contract ABIs
2.  `yarn build` - Builds and packages the SubQuery project into the `/dist` directory
3.  `docker-compose pull && docker-compose up` - Runs a Docker container with an indexer, PostgeSQL DB, and a query service. This requires [Docker to be installed](https://docs.docker.com/engine/install) and running locally. The configuration for this container is set from your `docker-compose.yml`

You can observe the three services start, and once all are running (it may take a few minutes on your first start), please open your browser and head to [http://localhost:3000](http://localhost:3000) - you should see a GraphQL playground showing with the schemas ready to query. [Read the docs for more information](https://academy.subquery.network/run_publish/run.html) or [explore the possible service configuration for running SubQuery](https://academy.subquery.network/run_publish/references.html).

## Query your project

For this project, you can try to query with the following GraphQL code to get a taste of how it works.

```graphql
{
  query {
    transfers(first: 2) {
      totalCount
      nodes {
        id
        blockNumber
        amount
        date
        to {
          id
          publicKey
          firstTransferBlock
          lastTransferBlock
        }
        from {
          id
          publicKey
          firstTransferBlock
          lastTransferBlock
        }
      }
    }
  }
}
```

In response, you can expect to receive something similar to this:

```graphql
{
  "data": {
    "query": {
      "transfers": {
        "nodes": [
          {
            "id": "2770553-87",
            "blockNumber": 2770553,
            "amount": "230000000",
            "date": "2024-04-16T07:04:48",
            "to": {
              "id": "5dp98jb1tw6ddp8sekhi1uuu9d6j9br8stj5dnvstltn3ajy",
              "publicKey": "58,75,189,159,154,170,52,232,106,241,120,137,73,234,30,253,40,177,17,163,14,180,158,208,222,88,27,230,212,238,238,99",
              "firstTransferBlock": 2770553,
              "lastTransferBlock": 2770553
            },
            "from": {
              "id": "5fef8cmcydwkmzqhzptgetepjaig2mmjrx8x44vsehuv2irg",
              "publicKey": "140,75,251,17,241,118,34,30,212,76,136,53,67,118,233,61,14,174,245,211,248,65,30,193,196,197,212,241,164,224,194,57",
              "firstTransferBlock": 2770551,
              "lastTransferBlock": 2770553
            }
          },
          {
            "id": "2770558-60",
            "blockNumber": 2770558,
            "amount": "28189544",
            "date": "2024-04-16T07:05:48",
            "to": {
              "id": "5fnfza3ze5sqcttyi59h6mckopqz7jrxqnqc7pm4zaerdev3",
              "publicKey": "164,101,241,49,59,163,109,227,95,3,192,85,195,124,241,65,72,61,124,59,44,188,26,255,68,21,130,141,7,46,210,4",
              "firstTransferBlock": 2770558,
              "lastTransferBlock": 2770558
            },
            "from": {
              "id": "5g7et4qk6wgkhhhsrefz7frfe7wuh3zq6rylvami5dtmexg3",
              "publicKey": "178,223,220,83,96,245,190,44,39,63,221,188,136,203,245,167,232,203,137,73,135,154,235,213,39,143,185,201,109,37,186,82",
              "firstTransferBlock": 2770527,
              "lastTransferBlock": 2770563
            }
          }
        ]
      }
    }
  }
}
```

You can explore the different possible queries and entities to help you with GraphQL using the documentation draw on the right.

## What Next?

Take a look at some of our advanced features to take your project to the next level!

- [**Multi-chain indexing support**](https://academy.subquery.network/build/multi-chain.html) - SubQuery allows you to index data from across different layer-1 networks into the same database, this allows you to query a single endpoint to get data for all supported networks.
- [**Dynamic Data Sources**](https://academy.subquery.network/build/dynamicdatasources.html) - When you want to index factory contracts, for example on a DEX or generative NFT project.
- [**Project Optimisation Advice**](https://academy.subquery.network/build/optimisation.html) - Some common tips on how to tweak your project to maximise performance.
- [**GraphQL Subscriptions**](https://academy.subquery.network/run_publish/subscription.html) - Build more reactive front end applications that subscribe to changes in your SubQuery project.

## Need Help?

The fastest way to get support is by [searching our documentation](https://academy.subquery.network), or by [joining our discord](https://discord.com/invite/subquery) and messaging us in the `#technical-support` channel.