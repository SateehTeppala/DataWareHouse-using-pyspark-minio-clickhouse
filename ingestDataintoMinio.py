import requests
import pandas as pd
import boto3
from io import StringIO
from datetime import datetime


def readApi():
    getRespone = requests.get("https://random-data-generator.vercel.app/data")

    data = getRespone.json()

    df = pd.DataFrame(data)

    return df


def writeToMinio(new_df):
    MINIO_ENDPOINT = "http://localhost:9000"  # minio endpoint
    ACCESS_KEY = "minioadmin" # root_user_name
    SECRET_KEY = "minioadmin" # root_user_password
    BUCKET_NAME = "web3" #bucket name

    # Generate filename with current timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    FILE_NAME = f"data_{timestamp}.csv"

    # Initialize MinIO client
    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )

    # Write new file directly (no need to check for existing file)
    buffer = StringIO()
    new_df.to_csv(buffer, index=False)
    s3_client.put_object(Bucket=BUCKET_NAME, Key=FILE_NAME, Body=buffer.getvalue())

    # Read back and print
    obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=FILE_NAME)
    content = obj["Body"].read().decode('utf-8')
    buffer = StringIO(content)
    final_df = pd.read_csv(buffer)

    print(f"Created new file: {FILE_NAME}")
    print("Final DataFrame:\n", final_df.head(99999999))


if __name__ == '__main__':
    for i in range(10): 
        writeToMinio(readApi())
