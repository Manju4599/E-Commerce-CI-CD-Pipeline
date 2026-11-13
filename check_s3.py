import boto3

# Connect to the local Floci S3 service
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost.floci.io:4566",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1",
)

# List all buckets
response = s3.list_buckets()

print("Available S3 buckets:")
for bucket in response["Buckets"]:
    print(bucket["Name"])

# List files under the Bronze orders prefix
print("\nFiles in Bronze :")
response1 = s3.list_objects_v2(
    Bucket="de-learning-lake",
    Prefix="bronze/orders/",
)
response2 = s3.list_objects_v2(
    Bucket="de-learning-lake",
    Prefix="bronze/customers/",
)
response3 = s3.list_objects_v2(
    Bucket="de-learning-lake",
    Prefix="bronze/product/",
)
response4 = s3.list_objects_v2(
    Bucket="de-learning-lake",
    Prefix="bronze/order_items/",
)

for obj in response1.get("Contents", []):
    print("\nFiles in Orders:")
    print(f"File: {obj['Key']} | Size: {obj['Size']} bytes")
for obj in response2.get("Contents", []):
    print("\nFiles in Customers:")
    print(f"File: {obj['Key']} | Size: {obj['Size']} bytes")
for obj in response3.get("Contents", []):
    print("\nFiles in Product:")
    print(f"File: {obj['Key']} | Size: {obj['Size']} bytes")
for obj in response4.get("Contents", []):
    print("\nFiles in Order_items:")
    print(f"File: {obj['Key']} | Size: {obj['Size']} bytes")

print("\nFiles in Silver :")
response5 = s3.list_objects_v2(
    Bucket = "de-learning-lake",
    Prefix = "silver/orders/"
)
for obj in response5.get("Contents", []):
    print("\nFiles in Orders")
    print(f"File: {obj['Key']} | Size: {obj['Size']} bytes")

print("\nCheck Completed successfuly !")