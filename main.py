import os
import sys
from dataclasses import dataclass, field
from decimal import Decimal

from web3 import Web3

import requests
from pandas import DataFrame, option_context
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

INFURA_API_KEY = os.getenv("INFURA_API_KEY")
INFURA_URL = f"{os.getenv("INFURA_URL")}{INFURA_API_KEY}"


@dataclass
class Transaction:
    """Transaction class for storing transaction details."""

    from_address: str
    to_address: str
    value: str

@dataclass
class Block:
    """Block class for storing block details."""

    number: int
    hash: str
    transactions_list: str
    transactions: list[Transaction] = field(default_factory=list)


def get_block_details(block_identifier):
    """Get block details for a given block number or hash."""
    # JSON-RPC payload to fetch block details by block number or hash
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_getBlockByNumber",
        "params": [hex(block_identifier), False],
        "id": 1,
    }

    headers = {
        "Content-Type": "application/json"
    }

    # Make the HTTP POST request to Infura
    response = requests.post(INFURA_URL, headers=headers, json=payload)

    # Check if the request was successful
    if response.status_code == 200:
        block_details_raw = response.json()["result"]
        block_details = {
            "number": block_details_raw["number"],
            "hash": block_details_raw["hash"],
            "transactions_list": block_details_raw["transactions"],
        }
        return block_details
    else:
        print("Failed to fetch block details")
        print(response.text)
        return None


def get_transaction_details(tx_hash):
    """Get transaction details for a given transaction hash."""
    print(f"Getting transaction details for hash: {tx_hash}")

    payload = {
        "jsonrpc": "2.0",
        "method": "eth_getTransactionByHash",
        "params": [tx_hash],
        "id": 1,
    }

    headers = {
        "Content-Type": "application/json"
    }

    # Make the HTTP POST request to Infura
    response = requests.post(INFURA_URL, headers=headers, json=payload)

    # Check if the request was successful
    if response.status_code == 200:
        transaction_details_raw = response.json()["result"]
        # Convert Wei to Ether
        value = Web3.from_wei(int(transaction_details_raw["value"], 16), 'ether')
        transaction_details = {
            "from_address": transaction_details_raw["from"],
            "to_address": transaction_details_raw["to"],
            "value": value,
        }
        return transaction_details
    else:
        print(f"Failed to fetch transaction details for hash: {tx_hash}")
        print(response.text)


def sum_transaction_values(transactions) -> Decimal:
    """Sum the values of a list of transactions."""
    total_value = Decimal(0)
    for transaction in transactions:
        total_value += transaction.value
    return total_value


def count_unique_from_addresses(transactions) -> int:
    """Count the number of unique addresses in a list of transactions."""
    from_addresses = set()
    for transaction in transactions:
        from_addresses.add(transaction.from_address)
    return len(from_addresses)


def transactions_to_dataframe(transactions: list[Transaction]) -> DataFrame:
    # Extract transaction details into a list of dictionaries
    transaction_data = []
    for transaction in transactions:
        transaction_data.append({
            "from_address": transaction.from_address,
            "to_address": transaction.to_address,
            "value": transaction.value,
        })

    # Convert the list of dictionaries to a pandas DataFrame
    return DataFrame(transaction_data)


def address_count_and_value_sum_to_dataframe(address_count: int, sum_value: int) -> DataFrame:
    """Create a DataFrame with two columns: address_count and sum_value."""
    return DataFrame({"count_unique_addresses": [address_count], "sum_value": [sum_value]})


def write_to_parquet(dataframe: DataFrame, filename: str):
    """Write a DataFrame to a Parquet file."""
    # Write the DataFrame to a Parquet file
    dataframe.to_parquet(filename, engine="pyarrow", index=False)


if __name__ == "__main__":
    # Pull in the first argument from the command line
    block_number = int(sys.argv[1])
    block_raw = get_block_details(block_number)
    if block_raw:
        block = Block(**block_raw)
        for transaction in block.transactions_list:
            transaction_details = get_transaction_details(transaction)
            if transaction_details:
                block.transactions.append(Transaction(**transaction_details))

        total_value = sum_transaction_values(block.transactions)
        unique_from_addresses = count_unique_from_addresses(block.transactions)

        print(f"Total Value: {total_value}")
        print(f"Unique From Addresses: {unique_from_addresses}")

        # Write all the transactions to a parquet file
        transactions_dataframe = transactions_to_dataframe(block.transactions)
        parquet_filename = f"output/transactions.parquet"
        write_to_parquet(transactions_dataframe, parquet_filename)

        print(f"Transactions written to {parquet_filename}")

        # Write count addresses and sum value to a parquet file
        address_count_and_value_sum_dataframe = address_count_and_value_sum_to_dataframe(
            unique_from_addresses, total_value
        )
        parquet_filename = f"output/totals.parquet"
        write_to_parquet(address_count_and_value_sum_dataframe, parquet_filename)

        print(f"Address count and value sum written to {parquet_filename}")
