import duckdb


def etl():
    #Speed up processing
    duckdb.sql("PRAGMA threads=4;") 

    # Create Table
    query_create_table = """
    CREATE TABLE review_summary (
            business_id VARCHAR PRIMARY KEY,
            stars DOUBLE,
            useful DOUBLE,
            funny DOUBLE,
            cool DOUBLE,
            date date,
            count_review INT
        );
    """
    duckdb.sql(query_create_table)

    # Scenario 1: Initial Load
    query_create_table_temp = "CREATE TEMP TABLE review_temp AS SELECT * FROM read_json_auto('yelp_academic_dataset_review.json');"
    duckdb.sql(query_create_table_temp)


    duckdb.sql("""
           INSERT INTO review_summary  (
           SELECT business_id, AVG(stars) AS avg_stars, AVG(useful) AS avg_useful, AVG(funny) AS avg_funny, AVG(cool) AS avg_cool, MAX(DATE) AS latest_date, COUNT(business_id) AS count_review
           FROM review_temp 
           WHERE date BETWEEN '2018-01-01' AND '2019-01-01'
           GROUP BY business_id
           );
           """)


    # Check metrics after Scenario 1
    duckdb.sql("SELECT count(*) AS row_count, SUM(stars) AS sum_stars, SUM(useful) AS sum_useful, SUM(funny) AS sum_funny, SUM(cool) AS sum_cool, MAX(DATE) AS latest_date FROM review_summary").show(max_width=300, max_rows=300)

    duckdb.sql("SELECT date::date AS date, count(*) AS row_count, SUM(stars) AS sum_stars, SUM(useful) AS sum_useful, SUM(funny) AS sum_funny, SUM(cool) AS sum_cool, MAX(DATE) AS latest_date FROM review_summary GROUP BY date::date").show(max_width=300, max_rows=300)

    # Scenario 2: First Update
    duckdb.sql("""
           CREATE TEMP TABLE review_temp_01_02 AS (
           SELECT *
           FROM review_temp 
           WHERE date::date = '2018-01-02'::date - 1);
           -- SHOULD BE THIS IF RUNNING DAILY (current_date - 1)
           """)
    
    duckdb.sql("""
           WITH aggregate_temp AS (
           SELECT business_id, AVG(stars) AS avg_stars, AVG(useful) AS avg_useful, AVG(funny) AS avg_funny, AVG(cool) AS avg_cool, MAX(DATE) AS latest_date, COUNT(business_id) AS count_review
           FROM review_temp_01_02 
           GROUP BY business_id)

           
           INSERT OR REPLACE INTO review_summary (
           SELECT  
            U.business_id,
            COALESCE(((T.count_review * T.avg_stars) + (U.count_review * U.avg_stars)) / (U.count_review + T.count_review), U.avg_stars) AS avg_stars,
            COALESCE(((T.count_review * T.avg_useful) + (U.count_review * U.avg_useful)) / (U.count_review + T.count_review), U.avg_useful) AS avg_useful,
            COALESCE(((T.count_review * T.avg_funny) + (U.count_review * U.avg_funny)) / (U.count_review + T.count_review), U.avg_funny) AS avg_funny,
            COALESCE(((T.count_review * T.avg_cool) + (U.count_review * U.avg_cool)) / (U.count_review + T.count_review), U.avg_cool) AS avg_cool,
            GREATEST(U.latest_date, T.latest_date) as latest_date,
            COALESCE(T.count_review + U.count_review, U.count_review) AS count_review
           FROM aggregate_temp U
           LEFT JOIN review_summary T ON U.business_id = T.business_id);
           """)

    # Check metrics after Scenario 2
    duckdb.sql("SELECT count(*) AS row_count, SUM(stars) AS sum_stars, SUM(useful) AS sum_useful, SUM(funny) AS sum_funny, SUM(cool) AS sum_cool, MAX(DATE) AS latest_date FROM XXX").show(max_width=300, max_rows=300)

    duckdb.sql("SELECT date::date AS date, count(*) AS row_count, SUM(stars) AS sum_stars, SUM(useful) AS sum_useful, SUM(funny) AS sum_funny, SUM(cool) AS sum_cool, MAX(DATE) AS latest_date FROM XXX GROUP BY date::date").show(max_width=300, max_rows=300)

    # Scenario 3: Delayed Update
    duckdb.sql("""
           CREATE TEMP TABLE review_temp_backfill AS (
           SELECT *
           FROM review_temp 
           WHERE business_id not in (SELECT business_id FROM review_summary)
           );
           """)
    
    duckdb.sql("""
           WITH aggregate_temp AS (
           SELECT business_id, AVG(stars) AS stars, AVG(useful) AS useful, AVG(funny) AS funny, AVG(cool) AS cool, MAX(DATE) AS date, COUNT(business_id) AS count_review
           FROM review_temp_backfill 
           GROUP BY business_id)

           
           INSERT OR REPLACE INTO review_summary (
           SELECT  
            U.business_id,
            COALESCE(((T.count_review * T.stars) + (U.count_review * U.stars)) / (U.count_review + T.count_review), U.stars) AS avg_stars,
            COALESCE(((T.count_review * T.useful) + (U.count_review * U.useful)) / (U.count_review + T.count_review), U.useful) AS avg_useful,
            COALESCE(((T.count_review * T.funny) + (U.count_review * U.funny)) / (U.count_review + T.count_review), U.funny) AS avg_funny,
            COALESCE(((T.count_review * T.cool) + (U.count_review * U.cool)) / (U.count_review + T.count_review), U.cool) AS avg_cool,
            GREATEST(U.date, T.date) as date,
            COALESCE(T.count_review + U.count_review, U.count_review) AS count_review
           FROM aggregate_temp U
           LEFT JOIN review_summary T ON U.business_id = T.business_id);
           """)

    # Check metrics after Scenario 3
    duckdb.sql("SELECT count(*) AS row_count, SUM(stars) AS sum_stars, SUM(useful) AS sum_useful, SUM(funny) AS sum_funny, SUM(cool) AS sum_cool, MAX(DATE) AS latest_date FROM XXX").show(max_width=300, max_rows=300)

    duckdb.sql("SELECT date::date AS date, count(*) AS row_count, SUM(stars) AS sum_stars, SUM(useful) AS sum_useful, SUM(funny) AS sum_funny, SUM(cool) AS sum_cool, MAX(DATE) AS latest_date FROM XXX GROUP BY date::date").show(max_width=300, max_rows=300)


etl()