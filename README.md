# Reddit Data Pipeline
This is the project implementing data pipeline for retrieving data from Reddit for a topic r/dataengineering and storing it in a database.
This project demonstrates the use of Apache Airflow to schedule the data retrieval and storage process.
And, create table views that can answer the following questions:
1. What are the total number of authors and posts in this subreddit?
2. What is the average score?
3. How many posts are published per day?
4. What is the average number of comments per day?
5. Is there any interesting trend at this moment?

# Requirements
- Docker

# To run the project (scheduling)
1. Clone the repository
```bash
git clone {repo_url}
```

2. Change directory to the project
```bash
cd data-pipeline-reddit
```

3. Build the docker images with docker-compose
```bash
docker-compose build
```

4. Run the docker containers
```bash
docker-compose up
```

5. Check the logs
```bash
docker-compose logs -f
```

6. Check out our Airflow UI at
```bash
http://localhost:8080
```
log in with username: airflow, password: airflow. You will see the 2 DAGs we have created for this project.
You should set up
- Reddit API credentials as environment variables in the Airflow UI
- Database connection in the Airflow UI

7. Check out the database with adminer at
```bash
http://localhost:8090
```
log in with server: mydb, username: postgres, password: postgres, database: postgres.
When the DAGs are running, you will see the data being stored in the database.

# To answer the questions
number `1. to 4.` We can use the table `mart_summary_table` to answer these questions. Or, query as follows:
```sql
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
```

number `5.` We can use the table `mart_top_words_view` to answer this question.
