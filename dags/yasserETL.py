# Importation of librairies
from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from sqlalchemy import create_engine
from airflow.operators.python_operator import PythonOperator
from bs4 import BeautifulSoup
import pandas as pd
import os
import json
import html

# Static paths

FILE_PATH = '/opt/airflow/source/jobs.csv'
DESTINATION = '/opt/airflow/staging/extracted'
TRANSF = '/opt/airflow/staging/transformed'

# function to extract the csv document
def extract_data(file_path):
    data_frame = pd.read_csv(file_path)
    return data_frame


TABLES_CREATION_QUERY = """CREATE TABLE IF NOT EXISTS job (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title VARCHAR(225),
    industry VARCHAR(225),
    description TEXT,
    employment_type VARCHAR(125),
    date_posted DATE
);

CREATE TABLE IF NOT EXISTS company (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    name VARCHAR(225),
    link TEXT,
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS education (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    required_credential VARCHAR(225),
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS experience (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    months_of_experience INTEGER,
    seniority_level VARCHAR(25),
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS salary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    currency VARCHAR(3),
    min_value NUMERIC,
    max_value NUMERIC,
    unit VARCHAR(12),
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS location (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    country VARCHAR(60),
    locality VARCHAR(60),
    region VARCHAR(60),
    postal_code VARCHAR(25),
    street_address VARCHAR(225),
    latitude NUMERIC,
    longitude NUMERIC,
    FOREIGN KEY (job_id) REFERENCES job(id)
)
"""
# task that contain the function to create tables

def create_tables():
    create_tables = SqliteOperator(
        task_id="create_tables",
        sqlite_conn_id="sqlite_default",
        sql=TABLES_CREATION_QUERY
    )


# task that contain the function to extract files from csv
def extract():
    """Extract data from jobs.csv."""
    extracted_data = extract_data(FILE_PATH)

    # Extraction de la colonne "contexte" specifié
    context_column = extracted_data['context']

    for index, context_item in enumerate(context_column):
        file_path = os.path.join(DESTINATION, f'extracted_{index}.txt')
        with open(file_path, 'w') as file:
            file.write(str(context_item))
    return os.listdir(DESTINATION)

# task that contain the function to transform the extracted files
def transform():
    """Clean and convert extracted elements to json."""
    extracted_dir = DESTINATION
    transformed_dir = TRANSF
    transformed_data = []

    for file_name in os.listdir(extracted_dir):
        file_path = os.path.join(extracted_dir, file_name)

        with open(file_path, 'r', encoding='utf-8') as file:
            file_content = file.read()

            # Vérifier si le fichier n'est pas vide
            if not file_content.strip():
                print(f"Le fichier {file_name} est vide. Ignoré.")
                continue

            try:
                
                extracted_data = json.loads(file_content)
                if "experienceRequirements" in extracted_data and "monthsOfExperience" in extracted_data["experienceRequirements"]:
                    transformed_item = {
                    "job": {
                    "title": extracted_data.get("title", ""),
                    "industry": extracted_data.get("industry", ""),
                    "description":clean_description(extracted_data.get("description", " ")),
                    "employment_type": extracted_data.get("employmentType", " "),
                    "date_posted": extracted_data.get("datePosted", ""),
                },
                "company": {
                    "name": extracted_data.get("hiringOrganization", {}).get("name", ""),
                    "link": extracted_data.get("hiringOrganization", {}).get("sameAs", ""),
                },
                "education": {
                    "required_credential": extracted_data.get("educationRequirements", {}).get("credentialCategory", ""),
                },
                "experience": {
                    "months_of_experience": int(extracted_data.get("experienceRequirements", {}).get("monthsOfExperience", 0)),
                    "seniority_level": extracted_data.get("experienceRequirements", {}).get("seniorityLevel", " "),
                },
                "salary": {
                    "currency": extracted_data.get("estimatedSalary", {}).get("currency", " "),
                    "min_value": int(extracted_data.get("estimatedSalary", {}).get("value", {}).get("minValue", 0)),
                    "max_value":int(extracted_data.get("estimatedSalary", {}).get("value", {}).get("maxValue", 0)),
                    "unit": extracted_data.get("estimatedSalary", {}).get("value", {}).get("unitText", " "),
                },
                "location": {
                    "country": extracted_data.get("jobLocation", {}).get("address", {}).get("addressCountry", ""),
                    "locality": extracted_data.get("jobLocation", {}).get("address", {}).get("addressLocality", ""),
                    "region": extracted_data.get("jobLocation", {}).get("address", {}).get("addressRegion", ""),
                    "postal_code": extracted_data.get("jobLocation", {}).get("address", {}).get("postalCode", ""),
                    "street_address": extracted_data.get("jobLocation", {}).get("address", {}).get("streetAddress", ""),
                    "latitude": extracted_data.get("jobLocation", {}).get("latitude", ""),
                    "longitude": extracted_data.get("jobLocation", {}).get("longitude", ""),
                },
            } 
                transformed_data.append(transformed_item)

                transformed_file_path = os.path.join(transformed_dir, f"{file_name.replace('.txt', '.json')}")
                with open(transformed_file_path, 'w', encoding='utf-8') as transformed_file:
                    json.dump(transformed_item, transformed_file, ensure_ascii=False, indent=2)

            except json.JSONDecodeError as e:
                print(f"Erreur lors de la lecture du fichier {file_name} : {str(e)}")

    return transformed_data

# task that contain the function to clear the description part
def clean_description(description):
    decoded_description = html.unescape(description)
    html_description = decoded_description.replace('<br>', ' ')
    # Use BeautifulSoup to remove other HTML tags
    soup = BeautifulSoup(html_description, 'html.parser')
    
    # Remove all HTML tags except for <br>
    for tag in soup.find_all(True):
        if tag.name != 'br':
            tag.replace_with('')

    # Get the cleaned description
    cleaned_description = soup.get_text(separator=' ', strip=True)
    return cleaned_description

# task that contain the function to load the transformed json files in the sqlLite database
def load():
    """Load data to sqlite database."""
    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_default')
    data_to_db = TRANSF

    # Connect to the SQLite database
    engine = create_engine(sqlite_hook.get_uri(), future=True)
    connection = engine.connect()

    try:
        
        for file_name in os.listdir(data_to_db):
            file_path = os.path.join(data_to_db, file_name)

        if os.path.getsize(file_path) > 0:
            with open(file_path, 'r', encoding='utf-8') as transformed_file:
                transformed_data = json.load(transformed_file)

                # Insert job data
                job_data = transformed_data['job']
                job_insert_query = """
                    INSERT INTO job (title, industry, description, employment_type, date_posted)
                    VALUES (?, ?, ?, ?, ?)
                """
                connection.execute(job_insert_query, (
                    job_data['title'],
                    job_data['industry'],
                    job_data['description'],
                    job_data['employment_type'],
                    job_data['date_posted']
                ))

                # Get the last inserted job ID
                job_id = connection.execute("SELECT last_insert_rowid()").fetchone()[0]

                # Insert company data
                company_data = transformed_data['company']
                company_insert_query = """
                    INSERT INTO company (job_id, name, link)
                    VALUES (?, ?, ?)
                """
                connection.execute(company_insert_query, (
                    job_id,
                    company_data['name'],
                    company_data['link']
                ))

                # Insert education data
                education_data = transformed_data['education']
                education_insert_query = """
                    INSERT INTO education (job_id, required_credential)
                    VALUES (?, ?)
                """
                connection.execute(education_insert_query, (
                    job_id,
                    education_data['required_credential']
                ))

                # Insert experience data
                experience_data = transformed_data['experience']
                experience_insert_query = """
                    INSERT INTO experience (job_id, months_of_experience, seniority_level)
                    VALUES (?, ?, ?)
                """
                connection.execute(experience_insert_query, (
                    job_id,
                    experience_data['months_of_experience'],
                    experience_data['seniority_level']
                ))

                # Insert salary data
                salary_data = transformed_data['salary']
                salary_insert_query = """
                    INSERT INTO salary (job_id, currency, min_value, max_value, unit)
                    VALUES (?, ?, ?, ?, ?)
                """
                connection.execute(salary_insert_query, (
                    job_id,
                    salary_data['currency'],
                    salary_data['min_value'],
                    salary_data['max_value'],
                    salary_data['unit']
                ))

                # Insert location data
                location_data = transformed_data['location']
                location_insert_query = """
                    INSERT INTO location (job_id, country, locality, region, postal_code, street_address, latitude, longitude)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """
                connection.execute(location_insert_query, (
                    job_id,
                    location_data['country'],
                    location_data['locality'],
                    location_data['region'],
                    location_data['postal_code'],
                    location_data['street_address'],
                    location_data['latitude'],
                    location_data['longitude']
                ))
        else:
            print(f"Le fichier {file_name} est vide. Ignoré.")

    except Exception as e:
        print(f"Erreur lors du chargement des données dans la base de données : {str(e)}")

    finally:
        connection.close()

# DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate a DAG with the specified default_args
dag = DAG(
    'ETLl2',
    default_args=default_args,
    description='A simple DAG to print "Hello, Airflow!"'
)


create_tables_task = PythonOperator(
    task_id='CREATE_TABLES',
    python_callable=create_tables,
    dag=dag,
)

extract_task = PythonOperator(
    task_id='EXTRACTIONS',
    python_callable=extract,
    dag=dag,
)
transform_task = PythonOperator(
    task_id='TRANSFORMATION',
    python_callable=transform,
    dag=dag,
)

load_task = PythonOperator(
    task_id='LOADING',
    python_callable=load,
    dag=dag,
)


create_tables_task >> extract_task >> transform_task >> load_task

if __name__ == "__main__":
    dag.cli()