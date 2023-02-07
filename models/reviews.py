import sys
import os
import shutil

import snowflake.snowpark.functions as f
from snowflake.snowpark.functions import col
from nltk.sentiment import SentimentIntensityAnalyzer
import nltk
import pandas as pd

IMPORT_DIR = sys._xoptions["snowflake_import_directory"]

BASE_TEMP_DIR = f'/tmp/nltk_data'
LOCAL_TEMP_DIR = f'/tmp/nltk_data/sentiment'
FILE_NAME = "vader_lexicon.zip"

def move_files():

    if os.path.exists(LOCAL_TEMP_DIR) == False:

        os.mkdir(BASE_TEMP_DIR)
        os.mkdir(LOCAL_TEMP_DIR)

        new_path = os.path.join(LOCAL_TEMP_DIR, FILE_NAME)
        original_path = os.path.join(IMPORT_DIR, FILE_NAME)
        shutil.copy(original_path, new_path)


def model(dbt, session):
    dbt.config(materialized = "table", packages = ["nltk", "pandas"], imports = ['@DBT_DEPS/nltk_data/sentiment/vader_lexicon.zip'])
    
    df_reviews = dbt.ref("stg_reviews")

    move_files()

    nltk.data.path.append(BASE_TEMP_DIR)

    pandas_df = df_reviews.to_pandas()    

    sia = SentimentIntensityAnalyzer()

    pandas_df["REVIEW_POSITIVE"] = pandas_df["REVIEW_TEXT"].apply(lambda x:sia.polarity_scores(x)['compound'] > 0)

    final_df = session.write_pandas(pandas_df, "write_pandas_table", auto_create_table=True, table_type="temp")

    return final_df.select(col("order_id"), col("review_text"), col("review_positive"))


