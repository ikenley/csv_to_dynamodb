import boto3
import csv
import os
from decimal import Decimal
from botocore.exceptions import ClientError


def load_csv_to_dynamodb(csv_file_path, table_name):
    """
    Load data from CSV file into DynamoDB table

    Args:
        csv_file_path (str): Path to the CSV file
        table_name (str): Name of the DynamoDB table
        region_name (str): AWS region name
    """

    # Initialize DynamoDB resource
    dynamodb = boto3.client("dynamodb")

    successful_inserts = 0
    failed_inserts = 0

    try:
        with open(csv_file_path, "r", newline="", encoding="utf-8") as csvfile:
            # Read CSV with DictReader to automatically handle headers
            reader = csv.DictReader(csvfile)

            print(f"Found columns: {reader.fieldnames}")

            # Process each row
            for row_num, row in enumerate(
                reader, start=2
            ):  # start=2 because row 1 is header
                try:
                    # Put item into DynamoDB
                    dynamodb.put_item(
                        Item={
                            "hash_key": {
                                "S": row["partition_key"],
                            },
                            "range_key": {
                                "S": "i",
                            },
                        },
                        TableName=table_name,
                    )
                    successful_inserts += 1

                    if successful_inserts % 100 == 0:
                        print(f"Processed {successful_inserts} records...")

                except ClientError as e:
                    print(
                        f"Error inserting row {row_num}: {e.response['Error']['Message']}"
                    )
                    print(f"Row data: {row}")
                    failed_inserts += 1
                except Exception as e:
                    print(f"Unexpected error on row {row_num}: {str(e)}")
                    print(f"Row data: {row}")
                    failed_inserts += 1

    except FileNotFoundError:
        print(f"Error: Could not find file {csv_file_path}")
        return
    except Exception as e:
        print(f"Error reading CSV file: {str(e)}")
        return

    print(f"\nLoad complete!")
    print(f"Successfully inserted: {successful_inserts} records")
    print(f"Failed inserts: {failed_inserts} records")


def main():
    # Configuration
    CSV_FILE_PATH = os.environ.get("CSV_FILE_PATH")
    TABLE_NAME = os.environ.get("TABLE_NAME")

    print(f"Loading data from {CSV_FILE_PATH} to DynamoDB table {TABLE_NAME}")

    # Load the data
    load_csv_to_dynamodb(CSV_FILE_PATH, TABLE_NAME)


if __name__ == "__main__":
    main()
