# EV infrastructure analysis

## Problem Statement 
Analyse the EV charging infrastructure to identify any patterns and make suggestions for improvement.
## Data Source
https://openchargemap.org/site/develop/api#/schemas/ConnectionInfo

## Data Architecture
![image](https://github.com/mudrap17/ev-pattern-analysis/assets/76879120/4e7a0d89-851e-4152-9d72-8ada3c911014)


Clarification: I have only used Free Tier services in both AWS and Snowflake.

So Glue is not for ETL but only the services like Glue Catalog and Crawler are utilised. I believe it can be confusing for understanding why Airflow was used over Glue ETL when all services are in AWS, it was just about the cost factor.

In summary, I'm using Airflow for ingesting from the API, Lambda is triggered when S3 bucket put object event occurs and then when there is a cleaned object available you can trigger the loading with Snowflake. I didn't use AWS Glue for ETL here, I only used Glue Crawler and Data Catalog for data exploration and ran SQL queries with Athena to make sure I have the right structure. If you see the code base, the connection with Snowflake was through SQL queries and not Glue. I'm working with free trials here so there are limitations, but you can use Glue if you are more comfortable and if that suits your use case!

Blog post: https://medium.com/@mudrapatel17/data-engineering-concepts-part-8-data-architecture-28104ec41690

Airflow script: ``extract_load.py``

Lambda function: ``lambda_function.py``

Snowflake script: ``snowflake.txt``

