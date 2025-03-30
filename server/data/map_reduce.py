import argparse
from pyspark.sql import SparkSession
from pyspark.sql import Row


def process_twitter_data(input_path, output_path):
    """
    Processes Twitter dataset to calculate follower and followee counts.
    Saves the result as a CSV file in S3.
    """
    # Initialize Spark
    spark = SparkSession.builder.appName("TwitterFollowerCount").getOrCreate()
    sc = spark.sparkContext

    # Load data from S3
    lines = sc.textFile(input_path)

    # Map: Emit (user, 1) for followers and followees
    followers = lines.map(lambda line: (line.split()[1], 1))  # Count followers
    followees = lines.map(lambda line: (line.split()[0], 1))  # Count followees

    # Reduce: Sum counts for each user
    follower_counts = followers.reduceByKey(lambda a, b: a + b)
    followee_counts = followees.reduceByKey(lambda a, b: a + b)

    # Join results and convert to DataFrame
    result = follower_counts.join(followee_counts)
    result_df = spark.createDataFrame(result.map(lambda x: Row(
        user_id=x[0], followers=x[1][0], followees=x[1][1])))

    # Save as CSV
    result_df.write.mode("overwrite").csv(output_path, header=True)

    print("Processing completed. Output stored as CSV in S3.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_path', required=True,
                        help="S3 input path for the dataset.")
    parser.add_argument('--output_path', required=True,
                        help="S3 output path for results.")
    args = parser.parse_args()

    process_twitter_data(args.input_path, args.output_path)
