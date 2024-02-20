import pandas as pd
import numpy as np
import datetime
from minio import Minio
from minio.error import S3Error
import datetime
import json
import os

client = Minio(
    "localhost:9000",
    secure=False,
    access_key="minio",
    secret_key="minio123"
)
found = client.bucket_exists("warehouse")


if not found:
    client.make_bucket("warehouse")
else:
    print("Bucket 'warehouse' existe déjà")

client.fput_object("warehouse", "elem2.parquet", "/home/hedi/PycharmProjects/pythonProject/elem/.part-00000-0c7fcf4b-53ca-443f-97b4-be30207f6f7f-c000.snappy.parquet.crc")

