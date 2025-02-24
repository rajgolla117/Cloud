from fastapi import FastAPI, File, UploadFile, HTTPException
import boto3
import os
from concurrent.futures import ThreadPoolExecutor
import uvicorn

#FastAPI app
app = FastAPI()

#AWS credentials and resources

REGION = "us-east-1"

AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')

S3_BUCKET_NAME = "1217986809-in-bucket"
SDB_DOMAIN = "1217986809-simpleDB"

#Initialize AWS clients
session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=REGION
)
s3 = session.client('s3')
sdb = session.client('sdb')

#Thread pool for concurrency
executor = ThreadPoolExecutor(max_workers=5)

def upload_to_s3(file: UploadFile, file_name: str):
    """Uploads file to S3."""
    file_bytes = file.file.read()
    s3.put_object(Bucket=S3_BUCKET_NAME, Key=file_name, Body=file_bytes)

def query_simpledb(image_name: str):
    """Queries SimpleDB for classification results."""
    query = f"SELECT * FROM `{SDB_DOMAIN}` WHERE ItemName() = '{image_name}'"
    response = sdb.select(SelectExpression=query)
    return response['Items'][0]['Attributes'][0]['Value']

@app.post("/")
async def classify(inputFile: UploadFile = File(...)):
    file_name = inputFile.filename
    image_name = os.path.splitext(file_name)[0]

    # Run S3 upload and SimpleDB query concurrently
    future_s3 = executor.submit(upload_to_s3, inputFile, file_name)
    future_db = executor.submit(query_simpledb, image_name)

    uploaded_file = future_s3.result()  
    classification = future_db.result() 
    print(classification)
    if classification is None:
        raise HTTPException(status_code=404, detail="No classification found")

    return f"{image_name}:{classification}"

# Run the FastAPI app with Uvicorn
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
