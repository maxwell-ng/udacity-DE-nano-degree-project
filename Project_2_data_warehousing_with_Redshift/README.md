# The Purpose of Sparkify Analytical Database

The database built for Sparkify serves as an analytical foundation for the startup to gain insights and make data-driven decisions. Its purpose is to store and organize the relevant data in a structured manner, enabling efficient querying and analysis.
For context:
1. **Understanding User Behavior**: By capturing data such as songplays, user details, and timestamps, Sparkify can analyze user behaviour patterns. They can determine popular songs, identify user preferences, and understand usage patterns based on location, gender, and user level. This information can help Sparkify optimize their services, personalize recommendations, and tailor their offerings to enhance user satisfaction.
2. **Business Performance Analysis**: The database allows Sparkify to track key performance indicators (KPIs) such as total songplays, user demographics, and user churn rates. By analyzing these metrics over time, Sparkify can evaluate the success of marketing campaigns, monitor user acquisition and retention efforts, and make data-driven decisions to drive business growth.
3. **Optimizing User Experience**: The database's time dimension table enables Sparkify to analyze user activity trends over specific time periods, such as hourly, daily, weekly, or monthly patterns. This information helps Sparkify identify peak usage times, optimize server capacity, plan maintenance windows, and deliver a seamless user experience without performance bottlenecks.
4. **Improving Content Curation**: The database's dimension tables, such as songs and artists, provide essential information about the content available on the platform. Analyzing song attributes, genres, artist popularity, and user engagement with different types of content can help Sparkify curate and deliver a diverse and appealing music library. This contributes to increased user engagement and retention.

## Justification of Database Schema and ETL Pipeline

**Database Schema Design:**
The database schema design for Sparkify's analytics database follows a star schema model, which consists of a central fact table (songplay) surrounded by dimension tables (users, songs, artists, time).

1. **Fact Table (songplay):** The songplay table serves as the central fact table, capturing the core events of user interactions (e.g., song plays) with the system. It contains foreign keys that establish relationships with dimension tables. This design allows efficient and fast aggregations and analysis of user activity.
2. **Dimension Tables:** The dimension tables (users, songs, artists, time) provide additional context and descriptive attributes related to the events recorded in the fact table. They enable detailed analysis and drill-down capabilities. By separating these attributes into dimension tables, redundancy is reduced, and data integrity is maintained.

**ETL Pipeline:**
The Extract, Transform, Load (ETL) pipeline is responsible for extracting data from S3 buckets, loading it in raw format into staging tables, then transforming it into a suitable form, and loading it into fact and dimension tables. Here's a justification for the chosen ETL pipeline approach:

1. **Extract:** The ETL pipeline extracts data from relevant S3 buckets to Sparkify database, such as song streaming log files and song data.
2. **Transform:** The transformation stage involves cleaning, filtering, and structuring the extracted data. This process ensures data consistency, resolves inconsistencies, performs data type conversions, and applies business rules to derive meaningful insights. 
3. **Load:** The transformed data is loaded into the various fact and dimension tables, following the designed schema. 
In summary, the chosen database schema design and ETL pipeline provide a solid foundation for Sparkify's analytical goals. They promote efficient querying, data integrity, scalability, and flexibility, enabling Sparkify to gain meaningful insights and make informed business decisions based on their data assets.

## Description of the files

1. the `dwh.cfg` contains configuration variables such as AWS keys and Redshift Cluster details.
2. the `sql_queries.py` contains all SQL queries for dropping, creating and inserting queries.
3. the `create_tables.py` contains script that drops all tables and create new tables.
4. the `etl.py` performs loading data from S3 buckets into staging tables and then insert data into fact and dimension tables.

## How To Run This Pipeline
1. Clone the repository.
2. Create a redshift cluster and attach read access permission to S3.
3. Open the TCP port of the cluster endpoint for public access.
4. Edit the `dwh.cfg` file and add your cluster details: HOST, DB_NAME, DB_USER, DB_PASSWORD
5. In the root folder, first run the `create_tables.py` file and `etl.py` file subsequently.

## Example queries

Retrieve the top 5 users with the highest number of songplay:
<code>
    SELECT u.user_id, u.first_name, u.last_name, COUNT(*) AS songplay_count
    FROM songplay sp
    JOIN users u ON sp.user_id = u.user_id
    GROUP BY u.user_id, u.first_name, u.last_name
    ORDER BY songplay_count DESC
    LIMIT 5;
</code>
Result:
![alt text](https://github.com/maxwell-ng/udacity-DE-nano-degree-project/blob/main/Project_2_data_warehousing_with_Redshift/img/result%201.jpg?raw=true)
Find the number of songplays by weekday:
<code>
    SELECT t.weekday, COUNT(*) AS songplay_count
    FROM songplay s
    JOIN time t ON s.start_time = t.start_time
    GROUP BY t.weekday
    ORDER BY t.weekday;
</code>
Result:
![alt txt](https://github.com/maxwell-ng/udacity-DE-nano-degree-project/blob/main/Project_2_data_warehousing_with_Redshift/img/result%202.jpg?raw=true)

Get the distribution of users by gender:
<code>
    SELECT u.gender, COUNT(*) AS songplay_count
    FROM songplay s
    JOIN users u ON s.user_id = u.user_id
    GROUP BY u.gender;
</code>
Result:
![alt txt](https://github.com/maxwell-ng/udacity-DE-nano-degree-project/blob/main/Project_2_data_warehousing_with_Redshift/img/result%203.jpg?raw=true)
