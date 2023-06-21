from airflow.decorators import dag, task, task_group
from cassandra.query import UNSET_VALUE
from airflow.models import Variable
from airflow.providers.apache.cassandra.hooks.cassandra import CassandraHook

from typing import List, Dict, Optional, Union
from datetime import datetime
import praw
import pendulum
import logging
import uuid

import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.window as W
import pyspark.sql.types as T

import unicodedata, string, re, os
import nltk
from nltk.corpus import stopwords
from nltk.sentiment import SentimentIntensityAnalyzer
from nltk.util import ngrams


logger = logging.getLogger(__name__)

def make_uuid():
    return F.udf(lambda: str(uuid.uuid1()), T.StringType())()

@F.udf(returnType=T.StringType())
def clean(text):
    text = text.lower()
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8', 'ignore')
    text = re.sub('\[.*?\]', ' ', text)
    text = re.sub('https?://\S+|www\.\S+', '', text)
    text = re.sub('<.*?>+', '', text)
    text = re.sub('[%s]' % re.escape(string.punctuation), '', text)
    text = re.sub('\n', ' ', text)
    text = re.sub('\r', '', text)
    text = re.sub('\w*\d\w*', '', text)
    return text


@F.udf(returnType=T.FloatType())
def sentiment_score(text):
    analyzer = SentimentIntensityAnalyzer()
    sentiment = analyzer.polarity_scores(text)
    return sentiment['compound']


@dag(
    dag_id="reddit_sentiment_analysis",
    start_date=pendulum.datetime(2023,6,6),
    catchup=False
)
def reddit_sen_analysis():
    
    @task()
    def sentiment_post_titles():
        MAX_MEMORY = '15G'

        conf = pyspark.SparkConf().setMaster("local[*]") \
                .set('spark.executor.heartbeatInterval', 10000) \
                .set('spark.network.timeout', 10000) \
                .set("spark.core.connection.ack.wait.timeout", "3600") \
                .set("spark.executor.memory", MAX_MEMORY) \
                .set("spark.driver.memory", MAX_MEMORY) \
                .set('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.3.0") \
                .set('spark.cassandra.connection.host', "cassandra_cassandra_1") \
                .set('spark.cassandra.connection.port', '9042') \
                .set('spark.cassandra.output.consistency.level','ONE')
        
        spark = SparkSession \
            .builder \
            .appName("title_sentiment") \
            .config(conf=conf) \
            .getOrCreate()
        
        nltk.download('vader_lexicon')
        
        df_posts = spark.read. \
            format("org.apache.spark.sql.cassandra"). \
            options(table="posts", keyspace="reddit").load()
        
        df_posts_scores = spark.read. \
            format("org.apache.spark.sql.cassandra"). \
            options(table="posts_sentiment_score", keyspace="reddit").load()
        
        mod_df = df_posts.na.drop(subset=["title"]).\
            join(df_posts_scores, df_posts_scores.post_uid == df_posts.uuid, "left_anti"). \
            select(F.col("uuid"), 
                   F.col("subreddit"), 
                   F.col("api_timestamp"), 
                   clean(F.col("title")).alias('clean_text'))

        score = mod_df.select(F.col("uuid").alias("post_uid"), 
                              F.col("subreddit"), 
                              F.col("api_timestamp"), 
                              sentiment_score(F.col("clean_text")).alias("sentiment_score_title"))
        
        score.write.format("org.apache.spark.sql.cassandra")\
                .options(table="posts_sentiment_score", keyspace="reddit").mode("append").save()
        

    @task()
    def sentiment_comment_titles():
        MAX_MEMORY = '15G'

        conf = pyspark.SparkConf().setMaster("local[*]") \
                .set('spark.executor.heartbeatInterval', 10000) \
                .set('spark.network.timeout', 10000) \
                .set("spark.core.connection.ack.wait.timeout", "3600") \
                .set("spark.executor.memory", MAX_MEMORY) \
                .set("spark.driver.memory", MAX_MEMORY) \
                .set('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.3.0") \
                .set('spark.cassandra.connection.host', "cassandra_cassandra_1") \
                .set('spark.cassandra.connection.port', '9042') \
                .set('spark.cassandra.output.consistency.level','ONE')
        
        spark = SparkSession \
            .builder \
            .appName("comment_sentiment") \
            .config(conf=conf) \
            .getOrCreate()
        
        nltk.download('vader_lexicon')
        
        df_comments = spark.read. \
            format("org.apache.spark.sql.cassandra"). \
            options(table="comments", keyspace="reddit").load()
        
        df_comments_scores = spark.read. \
            format("org.apache.spark.sql.cassandra"). \
            options(table="comments_sentiment_score", keyspace="reddit").load()
        
        mod_df = df_comments.na.drop(subset=["body"]).\
            join(df_comments_scores, df_comments_scores.comment_uid == df_comments.uuid, "left_anti"). \
            select(F.col("uuid"), 
                   F.col("subreddit"), 
                   F.col("api_timestamp"), 
                   clean(F.col("body")).alias('clean_text'))
        
        logging.info(mod_df.show())


        score = mod_df.select(F.col("uuid").alias("comment_uid"), 
                              F.col("subreddit"), 
                              F.col("api_timestamp"), 
                              sentiment_score(F.col("clean_text")).alias("sentiment_score_title"))
        
        score.write.format("org.apache.spark.sql.cassandra")\
                .options(table="comments_sentiment_score", keyspace="reddit").mode("append").save()
        
    
    @task()
    def sentiment_post_titles_avg():
        MAX_MEMORY = '15G'

        conf = pyspark.SparkConf().setMaster("local[*]") \
                .set('spark.executor.heartbeatInterval', 10000) \
                .set('spark.network.timeout', 10000) \
                .set("spark.core.connection.ack.wait.timeout", "3600") \
                .set("spark.executor.memory", MAX_MEMORY) \
                .set("spark.driver.memory", MAX_MEMORY) \
                .set('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.3.0") \
                .set('spark.cassandra.connection.host', "cassandra_cassandra_1") \
                .set('spark.cassandra.connection.port', '9042') \
                .set('spark.cassandra.output.consistency.level','ONE')
        
        spark = SparkSession \
            .builder \
            .appName("title_sentiment_avg") \
            .config(conf=conf) \
            .getOrCreate()
        
        
        hours = lambda i: i * 60 * 60
        w = (W.Window.partitionBy("subreddit").orderBy(F.col("api_timestamp").cast('long')).rangeBetween(-hours(5), 0))

        df_posts_scores = spark.read. \
            format("org.apache.spark.sql.cassandra"). \
            options(table="posts_sentiment_score", keyspace="reddit").load()
        
        
        avg_score = df_posts_scores.select(F.col("api_timestamp").alias("ingest_timestamp"),
                                           F.col("subreddit"),
                                           F.avg("sentiment_score_title").\
                                           over(w).alias("sentiment_score_avg")). \
                                           withColumn("uuid", make_uuid())
        
        avg_score.write.format("org.apache.spark.sql.cassandra")\
                .options(table="posts_sentiment_score_avg", keyspace="reddit").mode("append").save()
        
    @task()
    def sentiment_comments_avg():
        MAX_MEMORY = '15G'

        conf = pyspark.SparkConf().setMaster("local[*]") \
                .set('spark.executor.heartbeatInterval', 10000) \
                .set('spark.network.timeout', 10000) \
                .set("spark.core.connection.ack.wait.timeout", "3600") \
                .set("spark.executor.memory", MAX_MEMORY) \
                .set("spark.driver.memory", MAX_MEMORY) \
                .set('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.3.0") \
                .set('spark.cassandra.connection.host', "cassandra_cassandra_1") \
                .set('spark.cassandra.connection.port', '9042') \
                .set('spark.cassandra.output.consistency.level','ONE')
        
        spark = SparkSession \
            .builder \
            .appName("title_sentiment_avg") \
            .config(conf=conf) \
            .getOrCreate()
        
        
        hours = lambda i: i * 60 * 60
        w = (W.Window.partitionBy("subreddit").orderBy(F.col("api_timestamp").cast('long')).rangeBetween(-hours(5), 0))

        df_comments_scores = spark.read. \
            format("org.apache.spark.sql.cassandra"). \
            options(table="comments_sentiment_score", keyspace="reddit").load()
        
        
        avg_score = df_comments_scores.select(F.col("api_timestamp").alias("ingest_timestamp"),
                                           F.col("subreddit"),
                                           F.avg("sentiment_score_title").\
                                           over(w).alias("sentiment_score_avg")). \
                                           withColumn("uuid", make_uuid())
        
        avg_score.write.format("org.apache.spark.sql.cassandra")\
                .options(table="comments_sentiment_score_avg", keyspace="reddit").mode("append").save()


    sentiment_post_titles() >> sentiment_post_titles_avg()

    sentiment_comment_titles() >> sentiment_comments_avg()

summary = reddit_sen_analysis()