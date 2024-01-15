# JediSwap-v2-indexer

## Introduction

The repository stores three services:

1. `Indexer` collects the blockchain data and put it to MongoDb collections. It's using the [Apibara](https://www.apibara.com/) tool.
2. `Data transformer` which procces and transform data collected by the `indexer`. It creates or updates data in MongoDb so it can be used directly by the `server` app. Services should run periodically to processes the new data.
3. `Server` which is used to create a GraphQL APIs. In this stage the service reads data from the DB without any major processing.
 
## Set up

### Indexer

1. Adjust environment variables in `indexer/env-goerli` file
2. Create an account in [Apibara](https://app.apibara.com/auth/register)
3. Set apibara auth token as env variable

```
export AUTH_TOKEN=<your_apibara_key>
```

4. Run the indexer

```
cd indexer
docker compose up
```

### Server

#### Prerequisite

Create .env file and set variables:

```
cd server
vi .env
```
```
MONGO_URL=
MONGO_DB=
NETWORK=
RPC_URL=
```

Note:
- `MONGO_DB` should have the same value as `DB_NAME` in the indexer
- `testnet` or `mainnet` are allowed values for the `NETWORK` env variables

#### Data transformer for pools

1. Run the commnad to set up collections (tokens, factory, pools) in DB

```
poetry run server init
```

2. Run the commnad to processed the indexer data

```
poetry run server process
```

#### Data transformer for positions

1. Run the commnad to set up collections (tokens, factory, pools) in DB

```
poetry run server init
```

2. Run the commnad to processed the indexer data

```
poetry run server positions
```

### Server

1. Start the server

```
poetry run server graphql
```