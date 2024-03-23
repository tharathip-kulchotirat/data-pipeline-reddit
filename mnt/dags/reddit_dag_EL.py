from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone

def _extract_data(**context):
    """
    Extract data from Reddit API.
    """
    import praw
    
    CLIENT_ID = Variable.get("REDDIT_CLIENT_ID")
    CLIENT_SECRET = Variable.get("REDDIT_CLIENT_SECRET")
    PASSWORD = Variable.get("REDDIT_PASSWORD")
    USER_AGENT = "kuanggy test app"
    USERNAME = "kuanggy"
    
    reddit = praw.Reddit(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        password=PASSWORD,
        user_agent=USER_AGENT,
        username=USERNAME,
    )
    topic = "dataengineering"
    subreddit = reddit.subreddit(topic)
    
    # Total number of authors in the subreddit for today
    authors = [(author.id, author.name) for author in set([submission.author for submission in subreddit.top(time_filter='day')])]

    # Total number of posts in the subreddit for today
    posts = [(submission.id, submission.title, submission.score, submission.author.id, int(submission.created_utc)) for submission in subreddit.top(time_filter='day')]

    # Total number of comments in the subreddit for today
    comments = []
    for submission in subreddit.top(time_filter='day'):
        for comment in submission.comments:
            comments.append((comment.id, comment.body, submission.id, comment.author.id, int(comment.created_utc)))

    # Push the extracted data to XCom
    context['ti'].xcom_push(key='authors', value=authors)
    context['ti'].xcom_push(key='posts', value=posts)
    context['ti'].xcom_push(key='comments', value=comments)

def _validate_data(**context):
    """
    Validate the extracted data.
    """
    authors = context['ti'].xcom_pull(task_ids='extract_data', key='authors')
    posts = context['ti'].xcom_pull(task_ids='extract_data', key='posts')
    comments = context['ti'].xcom_pull(task_ids='extract_data', key='comments')
    
    # author.id and author.name are not null and must be strings
    assert all([isinstance(author[0], str) and isinstance(author[1], str) for author in authors])
    
    # submission.id is string, submission.title is string, submission.score is float, submission.author.id is string, submission.created_utc is int
    assert all([isinstance(post[0], str) and isinstance(post[1], str) and isinstance(post[2], int) and isinstance(post[3], str) and isinstance(post[4], int) for post in posts])
    
    # comment.id is string, comment.body is string, submission.id is string, comment.author.id is string, comment.created_utc is int
    assert all([isinstance(comment[0], str) and isinstance(comment[1], str) and isinstance(comment[2], str) and isinstance(comment[3], str) and isinstance(comment[4], int) for comment in comments])
    
    #  push
    context['ti'].xcom_push(key='authors', value=authors)
    context['ti'].xcom_push(key='posts', value=posts)
    context['ti'].xcom_push(key='comments', value=comments)

def _load_raw_data_to_postgres(**context):
    """
    Load raw data to Postgres.
    """
    from datetime import datetime
    
    authors = context['ti'].xcom_pull(task_ids='validate_data', key='authors')
    posts = context['ti'].xcom_pull(task_ids='validate_data', key='posts')
    comments = context['ti'].xcom_pull(task_ids='validate_data', key='comments')
    
    postgres_hook = PostgresHook(
        postgres_conn_id="postgres_conn",
        schema="postgres"
    )
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()
    
    # Create tables
    # 1. Author table: author_id, author_name
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS author (
        author_id TEXT PRIMARY KEY,
        author_name TEXT
    );
    """)
    # 2. Post table: post_id, post_title, post_score, post_author_id, post_created_at
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS post (
        post_id TEXT PRIMARY KEY,
        post_title TEXT,
        post_score INT,
        post_author_id TEXT,
        post_created_at INT
    );
    """)
    # 3. Comment table: comment_id, comment_body, comment_post_id, comment_author_id, comment_created_at
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS comment (
        comment_id TEXT PRIMARY KEY,
        comment_body TEXT,
        comment_post_id TEXT,
        comment_author_id TEXT,
        comment_created_at INT
    );
    """)
    
    # Insert data
    cursor.executemany("INSERT INTO author VALUES (%s, %s)", authors)
    cursor.executemany("INSERT INTO post VALUES (%s, %s, %s, %s, %s)", posts)
    cursor.executemany("INSERT INTO comment VALUES (%s, %s, %s, %s, %s)", comments)
    
    connection.commit()
    cursor.close()
    connection.close()
    
    print(f"Data loaded to Postgres at {datetime.now()}")

    
# DAG Extract, Load
with DAG(
    dag_id="reddit_dag_EL_pipeline",
    schedule_interval="@daily",
    start_date=timezone.datetime(2022, 1, 1),
    catchup=False,
) as dag:
    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=_extract_data,
        provide_context=True
    )
    
    validate_data = PythonOperator(
        task_id="validate_data",
        python_callable=_validate_data,
        provide_context=True
    )
    
    load_raw_data_to_postgres = PythonOperator(
        task_id="load_raw_data_to_postgres",
        python_callable=_load_raw_data_to_postgres,
        provide_context=True
    )
    
    extract_data >> validate_data >> load_raw_data_to_postgres