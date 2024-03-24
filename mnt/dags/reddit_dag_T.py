# Created: 2024-03-24
# By: Tharathip Kulchotirat
# Description: This DAG is used to serve data to the data mart for analysis of trends and daily summary of the subreddit.

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone

# Trends: Top 300 words in the subreddit per day (remove all stop words, punctuations, and special characters): word, count, date
def _get_top_300(**context):
    """
    Get the top 300 words in the subreddit per day from the postgres database.
    select post_title
    from post
    WHERE DATE(TO_TIMESTAMP(post_created_at)) = CURRENT_DATE;

    select comment_body
    from comment
    WHERE DATE(TO_TIMESTAMP(comment_created_at)) = CURRENT_DATE;
    """
    
    # Connect to the database
    postgres_hook = PostgresHook(
        postgres_conn_id="postgres_conn",
        schema="postgres"
    )
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()
    
    # Query from post table
    post_query = """
        SELECT post_title
        FROM post
        WHERE DATE(TO_TIMESTAMP(post_created_at)) = CURRENT_DATE;
    """
    posts = postgres_hook.get_pandas_df(post_query)
    
    # Query from comment table
    comment_query = """
        SELECT comment_body
        FROM comment
        WHERE DATE(TO_TIMESTAMP(comment_created_at)) = CURRENT_DATE;
    """
    comments = postgres_hook.get_pandas_df(comment_query)
    
    connection.commit()
    cursor.close()
    connection.close()
    
    context['ti'].xcom_push(key='posts', value=posts)
    context['ti'].xcom_push(key='comments', value=comments)
    
def _load_top_300(**context):
    """
    Load the top 300 words in the subreddit per day to the postgres database.
    """
    import pandas as pd
    from collections import Counter
    import nltk
    from nltk.tokenize import word_tokenize
    from nltk.corpus import stopwords
    
    nltk.download('punkt')
    nltk.download('stopwords')
    
    posts = context['ti'].xcom_pull(task_ids='get_top_300', key='posts')
    comments = context['ti'].xcom_pull(task_ids='get_top_300', key='comments')
    
    # stack the post and comment data to the table column named 'text'
    posts.rename(columns={'post_title': 'text'}, inplace=True)
    comments.rename(columns={'comment_body': 'text'}, inplace=True)
    data = pd.concat([posts, comments], axis=0)
    data.columns = ['text']
    
    # Concatenate all text in the Series into a single string
    text = ' '.join(data['text'])
    
    # Tokenize the text into words
    tokens = word_tokenize(text)
    tokens = [word.lower() for word in tokens]
    
    # remove stopwords and punctuation
    stop_words = set(stopwords.words('english'))
    tokens = [word for word in tokens if word.isalnum() and word not in stop_words]
    
    # count
    word_counts = Counter(tokens)
    top_words = dict(word_counts.most_common(300)).items()
    top_words_df = pd.DataFrame(top_words, columns=['word', 'count'])
    top_words_df['analysis_date'] = pd.to_datetime('today').date()
    
    # create the view
    view_query = """
        CREATE OR REPLACE VIEW public.mart_top_words_view AS
        SELECT word, count, analysis_date
        FROM (
            SELECT word, count, analysis_date, 
                ROW_NUMBER() OVER (ORDER BY count DESC) AS rank
            FROM (
                SELECT unnest(ARRAY[%(words)s]) AS word,
                        unnest(ARRAY[%(counts)s]) AS count,
                        unnest(ARRAY[%(dates)s]) AS analysis_date
            ) AS word_counts
        ) AS ranked_words
        WHERE rank <= 300;
    """
    params = {
        'words': top_words_df['word'].values.tolist(),
        'counts': top_words_df['count'].values.tolist(),
        'dates': top_words_df['analysis_date'].values.tolist()
    }
    
    # Connect to the database
    postgres_hook = PostgresHook(
        postgres_conn_id="postgres_conn",
        schema="postgres"
    )
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(view_query, params)
    connection.commit()
    cursor.close()
    connection.close()
    
    
# What are the total number of authors and posts, average score in the subreddit per day?: number_of_authors, number_of_posts, average_score, date
def _get_daily_summary():
    """
    Get the daily summary of the subreddit from the postgres database.
    """
    # Connect to the database
    postgres_hook = PostgresHook(
        postgres_conn_id="postgres_conn",
        schema="postgres"
    )
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()
        
    # Create table if not exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.mart_summary_table (
            summary_id SERIAL PRIMARY KEY,
            number_of_authors INT,
            number_of_posts INT,
            number_of_comments INT,
            average_score FLOAT,
            date_of_summary DATE DEFAULT CURRENT_DATE
        );
    """)

    # Query
    query = """
        INSERT INTO public.mart_summary_table (number_of_authors, number_of_posts, number_of_comments, average_score, date_of_summary)
        SELECT 
            COUNT(DISTINCT author.author_id) AS number_of_authors,
            COUNT(DISTINCT post.post_id) AS number_of_posts,
            COALESCE(SUM(comment_count), 0) AS number_of_comments,
            AVG(post.post_score) AS average_score,
            CURRENT_DATE AS date_of_summary
        FROM post
        LEFT JOIN author ON post.post_author_id = author.author_id
        LEFT JOIN (
            SELECT comment_post_id, COUNT(*) AS comment_count
            FROM comment
            GROUP BY comment_post_id
        ) AS comments ON post.post_id = comments.comment_post_id
        WHERE DATE(TO_TIMESTAMP(post.post_created_at)) = CURRENT_DATE;
    """
    
    cursor.execute(query)
    
    connection.commit()
    cursor.close()
    connection.close()
    
# DAG definition

with DAG(
    dag_id="reddit_dag_T",
    schedule_interval="@daily",
    start_date=timezone.datetime(2022, 1, 1),
    catchup=False,
) as dag:
    
    get_top_300 = PythonOperator(
        task_id="get_top_300",
        python_callable=_get_top_300,
        provide_context=True
    )
    
    load_top_300 = PythonOperator(
        task_id="load_top_300",
        python_callable=_load_top_300,
        provide_context=True
    )
    
    get_daily_summary = PythonOperator(
        task_id="get_daily_summary",
        python_callable=_get_daily_summary
    )
    
    get_top_300 >> load_top_300 >> get_daily_summary