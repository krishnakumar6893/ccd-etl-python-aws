Root Challenge - https://www.pluralsight.com/resources/blog/cloud/cloudguruchallenge-python-aws-etl

Lookup blog - https://keertisurapaneni.medium.com/cloudguruchallenge-event-driven-python-on-aws-c1adbdec19ae

ny_url = https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv

jh_url = https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv

## No module found error


- We need to install required dependecies and package them and upload as lambda layer

- For instance, to fix psycopg2 error, we need to install psycopg2-binary and package it as a layer. This can be done in Cloud9 IDE in AWS. Refer https://www.youtube.com/watch?v=80h9lXE07z0 for more details

```bash
1. sudo amazon-linux-extras install python3.8

2. curl -O https://bootstrap.pypa.io/get-pip.py

3. python3.8 get-pip.py --user

4. sudo python3.8 -m pip install psycopg2-binary -t python/

5. zip -r dependancies.zip python
```

### <u>NOTES</u>

* Always use try-except block to catch exceptions and log them

# <u>STEPS</u>

1. Create a new Postgres RDS instance via python code
2. Create a python code called `etl.py` to extract data from the above two csv files and merge the required data
3. Create a lambda.py where we
    * Create the `notify` function
        - where we do SNS publish

    * Create the `main` function
        - where we call the `etl` function (first try block)
        - connect to Postgres RDS (second try block)
        - Create table if table doesn't exist (third try block)
        - Use condition if `query_results == 0`, insert data for the first time (fourth try block)
        - If `query_results != 0`, check for new data and insert it (fifth try block)
            - Fetching the Last Date from the Database Table
            - Comparing Dates Between the Database and the New Data
            - Checking if There Are New Rows to Insert
                - If yes, create a temporary table(hint: use same colums as old table) and insert the new data(hint: use same method to insert data as in the first time)
                - Now, join the old table and the temporary table and insert the new data into the old table(hint: we can use LEFT JOIN and INSERT INTO old table)
                - Drop the temporary table
                - Format and send the inserted rows to email
        - If no new data, send the email that the data is already up to date
    * Commit and close the Database connection

    * Notify for each below steps
        - Data transformation failed
        - Connection to Postgres failed
        - Unable to find info about db table
        - First data insertion into db table failed
        - Successfully first time data insertion into db table
        - Daily insertion of data failed
        - Send the inserted rows to email
        - else Your data is already up to date, good bye


# ETL Process Flowchart

```mermaid
graph TD
    A[Start] --> B[Create Postgres RDS Instance]
    B --> C[Create ETL.py]
    C --> D[Extract & Merge CSV Data]

    D --> E[Lambda Function]

    subgraph "Lambda Main Function"

        E --> F[Call ETL Function]
        F --> G[Connect to PostgreSQL RDS]
        G --> H{Table Exists?}

        H -->|No| I[Create Table]
        H -->|Yes| J[Check Query Results]

        I --> J

        J -->|Results = 0| K[First Time Insert]
        J -->|Results > 0| L[Check for New Data]

        L --> M[Fetch Last Date from DB]
        M --> N[Compare Dates]
        N --> O{New Data Exists?}

        O -->|Yes| P[Create Temp Table]
        P --> Q[Insert New Data to Temp]
        Q --> R[Join & Insert to Main Table]
        R --> S[Drop Temp Table]
        S --> T[Format Email with New Data]

        O -->|No| U[Prepare 'Up to Date' Email]

        T --> V[SNS Publish]
        U --> V
        K --> V
    end

    V --> W[Commit & Close DB Connection]
    W --> X[End]

    style E fill:#f9f,stroke:#333,stroke-width:2px
