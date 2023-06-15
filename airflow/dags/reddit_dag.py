from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from airflow.providers.apache.cassandra.hooks.cassandra import CassandraHook

from typing import List, Dict, Optional, Union
from datetime import datetime
import praw
import pendulum
import logging
import uuid


logger = logging.getLogger(__name__)

subreddits = [
        # "programming",
        "analytics", 
        "deeplearning", 
        "datascience", 
        "datasets", 
        "kaggle", 
        "learnmachinelearning", 
        "MachineLearning", 
        "statistics", 
        "artificial", 
        "AskStatistics", 
        "computerscience", 
        "computervision", 
        "dataanalysis", 
        "dataengineering", 
        "DataScienceJobs", 
        "datascienceproject", 
        "data", 
        "MLQuestions", 
        "rstats"
    ]


@dag(
    dag_id="reddit_programming_summary",
    start_date=pendulum.datetime(2023,6,6),
    catchup=False
)
def reddit_programming_summary():
    
    @task()
    def get_reddit_conn() -> Dict[str, str]:
        client_id: str = "XQXpU_NfWW_KrcOgj20DYQ"
        client_secret: str = "neTeBovf7jgHi73v8Qdtl8TKntz8ug"
        user_agent: str = "ubuntu:sentiment_app"
        
        return {
            "user_agent": user_agent,
            "client_id": client_id,
            "client_secret": client_secret
        }
        
        
    @task()
    def get_posts(r_conf: Dict[str, str], sub: str):
        client = praw.Reddit(
            user_agent = r_conf["user_agent"],
            client_id = r_conf["client_id"],
            client_secret = r_conf["client_secret"]
        )
        
        hook = CassandraHook(cassandra_conn_id="reddit")
        cluster = hook.get_cluster()        
        c_con = cluster.connect('reddit')
        n_posts = 0
        
        for submission in client.subreddit(sub).new(limit=2000):
            
            author_name = submission.author.name if submission.author else None
            submission_json: dict[str, Union[float, Optional[str]]] = {
                "id": submission.id,
                "name": submission.name,
                "author": author_name,
                "title": submission.title,
                "selftext": submission.selftext,
                "subreddit": submission.subreddit.display_name,
                "upvotes": submission.score,
                "downvotes": int(submission.score / submission.upvote_ratio) - submission.score,
                "over_18": submission.over_18,
                "timestamp": submission.created_utc,
                "url": submission.url,
            }
            
            query = f"SELECT * FROM {hook.keyspace}.posts WHERE id='{submission_json['id']}' ALLOW FILTERING;"
            result = c_con.execute(query)
            exists = result.one() is not None
            
            if not exists:
                logger.info(submission_json)
                n_posts += 1
                time_stamp = datetime.fromtimestamp(submission_json["timestamp"]) if isinstance(submission_json["timestamp"], float) else None
                c_con.execute(
                    """
                    INSERT INTO reddit.posts (
                        uuid,
                        id,
                        name,
                        author,
                        title,
                        selftext,
                        subreddit,
                        upvotes,
                        over_18,
                        downvotes,
                        url,
                        api_timestamp
                    )
                    VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        uuid.uuid1(),
                        submission_json["id"],
                        submission_json["name"],
                        submission_json["author"],
                        submission_json["title"],
                        submission_json["selftext"],
                        submission_json["subreddit"],
                        submission_json["upvotes"],
                        submission_json["over_18"],
                        submission_json["downvotes"],
                        submission_json["url"],
                        time_stamp,
                    )
                )
            
        logging.info(f"Found {n_posts} posts.")
    
    @task()
    def get_comments(r_conf: Dict[str, str], sub: str):
        client = praw.Reddit(
            user_agent = r_conf["user_agent"],
            client_id = r_conf["client_id"],
            client_secret = r_conf["client_secret"]
        )
        n_comments = 0
        
        hook = CassandraHook(cassandra_conn_id="reddit")
        cluster = hook.get_cluster()        
        c_con = cluster.connect('reddit')
        
        for comment in client.subreddit(sub).comments(limit=10000):    
            author_name = comment.author.name if comment.author else None
            comment_json: dict[str, Optional[str]] = {
                "id": comment.id,
                "name": comment.name,
                "author": author_name,
                "body": comment.body,
                "subreddit": comment.subreddit.display_name,
                "submission": comment.submission.id,
                "upvotes": comment.ups,
                "downvotes": comment.downs,
                "over_18": comment.over_18,
                "timestamp": comment.created_utc,
                "permalink": comment.permalink,
            }
            
            query = f"SELECT * FROM {hook.keyspace}.comments WHERE id='{comment_json['id']}' ALLOW FILTERING;"
            result = c_con.execute(query)
            exists = result.one() is not None
            
            if not exists:
                n_comments += 1
                logger.info(comment_json)
                time_stamp = datetime.fromtimestamp(comment_json["timestamp"]) if isinstance(comment_json["timestamp"], float) else None
                c_con.execute(
                    """
                    INSERT INTO reddit.comments (
                        uuid,
                        id,
                        name,
                        author,
                        body,
                        subreddit,
                        upvotes,
                        downvotes,
                        over_18,
                        permalink,
                        submission_id,
                        api_timestamp
                    )
                    VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        uuid.uuid1(),
                        comment_json["id"],
                        comment_json["name"],
                        comment_json["author"],
                        comment_json["body"],
                        comment_json["subreddit"],
                        comment_json["upvotes"],
                        comment_json["downvotes"],
                        comment_json["over_18"],
                        comment_json["permalink"],
                        comment_json["submission"],
                        time_stamp,
                    )
                )
                
        logger.info(f"Found {n_comments} comments.")
    
    r_conf = get_reddit_conn()
    get_posts.expand(r_conf=[r_conf], sub=subreddits)
    get_comments.expand(r_conf=[r_conf], sub=subreddits)
    


summary = reddit_programming_summary()