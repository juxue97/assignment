import pandas as pd
from io import StringIO

from configs.aws_s3_connection import S3Client


class SimpleStorageService:
    def __init__(self, bucket_name: str, prefix: str):
        s3Client = S3Client()
        self.s3_client = s3Client.s3_client
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.user_data = None  # Cache user data

    def list_csv_files(self):
        """List only CSV files in the given S3 bucket and prefix."""
        response = self.s3_client.list_objects_v2(
            Bucket=self.bucket_name, Prefix=self.prefix)
        return [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".csv")]

    def _read_csv_from_s3(self, file_key: str) -> pd.DataFrame:
        """Read a single CSV file from S3 into a Pandas DataFrame."""
        obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=file_key)
        return pd.read_csv(StringIO(obj["Body"].read().decode("utf-8")))

    def _get_s3_dataframe(self) -> pd.DataFrame:
        """Fetch all CSV files in the specified S3 path and merge them into one DataFrame."""
        csv_files = self.list_csv_files()
        dataframes = [self._read_csv_from_s3(file) for file in csv_files]

        return pd.concat(dataframes, ignore_index=True) if dataframes else pd.DataFrame()

    def load_user_data(self):
        """Load and cache user data from S3, transforming it into a dictionary for quick lookups."""
        if self.user_data is None:  # Load only once
            df = self._get_s3_dataframe()

            if df.empty:
                raise Exception("No valid user data found in S3.")

            # Convert user_id to string for consistency
            df["user_id"] = df["user_id"].astype(str)

            # Convert to dictionary for fast lookups
            self.user_data = df.set_index(
                "user_id")[["followers", "followees"]].to_dict(orient="index")

        return self.user_data
