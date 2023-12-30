# JediSwap-v2-indexer

## Introduction

The repository stores three services:

1. `Indexer` collects the blockchain data and put it to MongoDb collections. It's using the [Apibara](https://www.apibara.com/) tool.
2. `Data transformer` which procces and transform data collected by the `indexer`. It creates or updates data in MongoDb so it can be used directly by the `server` app. The service should run periodically to processes the new data.
3. `Server` which is used to create a GraphQL APIs. In this stage the service reads data from the DB without any major processing.
 
## Set up

### Indexer

1. Adjust environment variables in `indexer/env-goerli` file
2. Run the indexer

```
cd indexer
docker compose up
```

### Data transformer

1. Set up env variables:

```
export MONGO_URL=
export MONGO_DB=
export NETWORK=
```

Note:
- `MONGO_DB` should have the same value as `DB_NAME` in the indexer
- `testnet` or `mainnet` are allowed values for the `NETWORK` env variables

2. Run the commnad to create the `tokens` collection

```
cd server
poetry run init_db
```

3. Run the commnad to processed the indexer data

```
poetry run transform
```

### Server

1. Set up env variables:

```
export MONGO_URL=
export MONGO_DB=
```

Note:
- `MONGO_DB` should have the same value as `DB_NAME` in the indexer

2. Start the server

```
cd server
poetry run server
```