# eth-transactions

## Introduction
This is a simple example of how to pull data from the Ethereum blockchain and write it to a Parquet file using Infura.

## Env Variables
Create a `.env` file in the root of the project with the following contents:
```env
INFURA_API_KEY=YOUR_INFURA_API_KEY
INFURA_URL=<YOUR_INFURA_URL>
```

# Building and Running with Docker

## Building

To build the Docker image, run the following command from the root of the project:
```commandline
    docker build -t eth-transactions .
```

## Running
To run the Docker image, run the following command from the root of the project:

First create an output folder for the parquet files locally:
```commandline
    mkdir output
```
Then run the Docker image:
```commandline
    docker run -it --rm --env-file .env -v $(pwd)/output:/app/output eth-transactions <BLOCK_NUMBER>
```

# Possible improvements

- Use multithreading to pull data from the blockchain in parallel
- Allow passing in multiple block numbers to pull data from and store the parquet files per block
- Use a cloud storage service to store the parquet files