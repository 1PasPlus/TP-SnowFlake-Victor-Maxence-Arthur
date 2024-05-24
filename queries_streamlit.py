import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd

# Write directly to the app
st.title("LinkedIn Job Postings Analysis")

# Get the current session
session = get_active_session()

# Define SQL queries
queries = {
    "Top 10 Job Titles": """
        SELECT title, COUNT(*) AS job_count 
        FROM job_postings 
        GROUP BY title 
        ORDER BY job_count DESC 
        LIMIT 10;
    """,
    "Job Postings by Company Size": """
        SELECT c.company_size, COUNT(*) AS job_count 
        FROM job_postings jp 
        JOIN companies c ON jp.company_id = c.company_id 
        GROUP BY c.company_size 
        ORDER BY job_count DESC;
    """,
    "Job Postings by Industry": """
        SELECT i.industry_name, COUNT(ji.job_id) AS nombre_offres 
        FROM Job_Industries ji 
        JOIN Industries i ON ji.industry_id = i.industry_id 
        GROUP BY i.industry_name 
        ORDER BY nombre_offres DESC 
        LIMIT 20;
    """,
    "Job Postings by Work Type": """
        SELECT formatted_work_type, COUNT(job_id) AS nombre_offres 
        FROM JOB_POSTINGS 
        GROUP BY formatted_work_type 
        ORDER BY nombre_offres DESC;
    """,
    "Job Postings by Location": """
        SELECT location, COUNT(job_id) AS nombre_offres 
        FROM Job_postings 
        GROUP BY location 
        ORDER BY nombre_offres DESC 
        LIMIT 20;
    """
}

# Function to run a query and return a Pandas DataFrame
def run_query(query):
    return session.sql(query).to_pandas()

# Function to display chart and data
def display_chart_and_data(title, df, x_col, y_col):
    st.subheader(title)
    st.bar_chart(data=df, x=x_col, y=y_col)
    st.subheader("Underlying data")
    st.dataframe(df, use_container_width=True)

# Top 10 Job Titles
top_10_job_titles = run_query(queries["Top 10 Job Titles"])
display_chart_and_data("Top 10 Job Titles", top_10_job_titles, "TITLE", "JOB_COUNT")

# Jobs by Company Size
job_by_company_size = run_query(queries["Job Postings by Company Size"])
display_chart_and_data("Job Postings by Company Size", job_by_company_size, "COMPANY_SIZE", "JOB_COUNT")

# Jobs by Industry
job_by_industry = run_query(queries["Job Postings by Industry"])
display_chart_and_data("Job Postings by Industry", job_by_industry, "INDUSTRY_NAME", "NOMBRE_OFFRES")

# Jobs by Work Type
job_by_type = run_query(queries["Job Postings by Work Type"])
display_chart_and_data("Job Postings by Work Type", job_by_type, "FORMATTED_WORK_TYPE", "NOMBRE_OFFRES")

# Jobs by Location
job_by_location = run_query(queries["Job Postings by Location"])
display_chart_and_data("Job Postings by Location", job_by_location, "LOCATION", "NOMBRE_OFFRES")
