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
                "num_comments": submission.num_comments,
                "name": submission.name,
                "author": author_name,
                "title": submission.title,
                "selftext": submission.selftext,
                "subreddit": submission.subreddit.display_name,
                "score": submission.score,
                "over_18": submission.over_18,
                "timestamp": submission.created_utc,
                "url": submission.url,
            }
            
            query = f"SELECT * FROM {hook.keyspace}.posts WHERE id='{submission_json['id']}' ALLOW FILTERING;"
            # result = c_con.execute(query)
            # exists = result.one() is not None

            exists = None

            
            if not exists:
                logger.info(submission_json)
                n_posts += 1
                time_stamp = datetime.fromtimestamp(submission_json["timestamp"]) if isinstance(submission_json["timestamp"], float) else None
                ps = c_con.prepare( 
                    """
                    INSERT INTO reddit.posts (
                        uuid,
                        num_comments,
                        id,
                        name,
                        author,
                        title,
                        selftext,
                        subreddit,
                        score,
                        over_18,
                        url,
                        api_timestamp
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                )
                c_con.execute(
                    ps, (
                        uuid.uuid1(),
                        submission_json["num_comments"],
                        submission_json["id"],
                        submission_json["name"] if submission_json["name"] is not None else UNSET_VALUE,
                        submission_json["author"] if submission_json["author"] is not None else UNSET_VALUE,
                        submission_json["title"] if submission_json["title"] is not None else UNSET_VALUE,
                        submission_json["selftext"] if submission_json["selftext"] is not None else UNSET_VALUE,
                        submission_json["subreddit"],
                        submission_json["score"],
                        submission_json["over_18"],
                        submission_json["url"] if submission_json["url"] is not None else UNSET_VALUE,
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
            comment_json: Dict[str, Union[Optional[str], int]] = {
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
            # result = c_con.execute(query)
            # exists = result.one() is not None

            exists = None
            
            if not exists:
                n_comments += 1
                logger.info(comment_json)
                time_stamp = datetime.fromtimestamp(comment_json["timestamp"]) if isinstance(comment_json["timestamp"], float) else None

                ps = c_con.prepare( 
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
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                )

                c_con.execute(
                    ps, (
                        uuid.uuid1(),
                        comment_json["id"],
                        comment_json["name"] if comment_json["name"] is not None else UNSET_VALUE,
                        comment_json["author"] if comment_json["author"] is not None else UNSET_VALUE,
                        comment_json["body"] if comment_json["body"] is not None else UNSET_VALUE,
                        comment_json["subreddit"],
                        comment_json["upvotes"],
                        comment_json["downvotes"],
                        comment_json["over_18"],
                        comment_json["permalink"] if comment_json["permalink"] is not None else UNSET_VALUE,
                        comment_json["submission"] if comment_json["submission"] is not None else UNSET_VALUE,
                        time_stamp,
                    )
                )
                
        logger.info(f"Found {n_comments} comments.")
    
    r_conf = get_reddit_conn()
    get_posts.expand(r_conf=[r_conf], sub=subreddits)
    get_comments.expand(r_conf=[r_conf], sub=subreddits)
    

summary = reddit_programming_summary()