# JediSwap-v2-indexer

## Introduction

The repository stores three services:

1. `Indexer` collects the blockchain data and put it to MongoDb collections. It's using the [Apibara](https://www.apibara.com/) tool.
2. `Data transformer` which procces and transform data collected by the `indexer`. It creates or updates data in MongoDb so it can be used directly by the `server` app. Services should run periodically to processes the new data.
3. `Server` which is used to create a GraphQL APIs. In this stage the service reads data from the DB without any major processing.
 
## Run locally

### Indexer

1. Adjust environment variables in `env-<network>` file
2. Create an account in [Apibara](https://app.apibara.com/auth/register)
3. Set apibara auth token as env variable

```
export AUTH_TOKEN=<your_apibara_key>
```

4. Run the indexer

```
docker compose up
```

### Server

#### Prerequisite

Adjust environment variables in `env-<network>` file

#### Data transformer for events

```
poetry run server events
```

#### Data transformer for positions

```
poetry run server positions
```

#### Data transformer for lp leaderboard contest

Should be run after processing all events and positions by transformers

```
poetry run server leaderboard
```

### Server

Start the server

```
poetry run server graphql
```

### STRK rewards calculation script

Start the server

```
poetry run server strk-calculation
```

The output data is saved to `server/strk_rewards_data`

## Run all services via docker compose

1. Adjust environment variables in `env-<network>` file
2. Create an account in [Apibara](https://app.apibara.com/auth/register)
3. Set apibara auth token as env variable

```
export AUTH_TOKEN=<your_apibara_key>
```
4. Set RPC_URL and MONGODB_CONNECTION_STRING as env variable

```
export RPC_URL=<your_rpc_url>
export MONGODB_CONNECTION_STRING=<your_mongo_url>
```

4. Run `docker-compose.yml` or `docker-compose.prod.yml`

```
docker compose up
```