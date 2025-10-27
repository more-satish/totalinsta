import boto3
import zipfile
import io
from pyspark.context import SparkContext
from awsglue.context import GlueContext

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)

# Input and output S3 paths
input_path = "s3://amzn-satish-insta/amzn-raw/instagram-moresatish78332-2025-10-20-fosoBXaU.zip"
output_path = "s3://amzn-satish-insta/amzn-raw/unzipped/"

# Parse S3 path
def parse_s3_path(s3_path):
    s3_path = s3_path.replace("s3://", "")
    bucket = s3_path.split("/")[0]
    key = "/".join(s3_path.split("/")[1:])
    return bucket, key

bucket, key = parse_s3_path(input_path)

# Download ZIP from S3
s3 = boto3.client("s3")
zip_obj = s3.get_object(Bucket=bucket, Key=key)
buffer = io.BytesIO(zip_obj["Body"].read())

# Extract each file from ZIP and upload it back to S3
with zipfile.ZipFile(buffer, "r") as zip_ref:
    for file_name in zip_ref.namelist():
        file_data = zip_ref.read(file_name)
        s3.put_object(Bucket=bucket, Key=f"{output_path.split('s3://')[1].split('/',1)[1]}{file_name}", Body=file_data)

print("✅ ZIP file extracted and saved to output S3 location as-is.")
# code is done