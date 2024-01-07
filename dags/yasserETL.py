from datetime import timedelta, datetime
import pandas as pd
import json
import os
from bs4 import BeautifulSoup
from airflow.decorators import dag, task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from sqlalchemy import create_engine, Column, Integer, String, Text, Date, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class Job(Base):
    __tablename__ = 'job'
    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String(225))
    industry = Column(String(225))
    description = Column(Text)
    employment_type = Column(String(125))
    date_posted = Column(Date)

class Company(Base):
    __tablename__ = 'company'
    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(Integer, ForeignKey('job.id'))
    name = Column(String(225))
    link = Column(String)

class Education(Base):
    __tablename__ = 'education'
    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(Integer, ForeignKey('job.id'))
    required_credential = Column(String(225))

class Experience(Base):
    __tablename__ = 'experience'
    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(Integer, ForeignKey('job.id'))
    months_of_experience = Column(Integer)
    seniority_level = Column(String(25))

class Salary(Base):
    __tablename__ = 'salary'
    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(Integer, ForeignKey('job.id'))
    currency = Column(String(3))
    min_value = Column(Numeric)
    max_value = Column(Numeric)
    unit = Column(String(12))

class Location(Base):
    __tablename__ = 'location'
    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(Integer, ForeignKey('job.id'))
    country = Column(String(60))
    locality = Column(String(60))
    region = Column(String(60))
    postal_code = Column(String(25))
    street_address = Column(String(225))
    latitude = Column(Numeric)
    longitude = Column(Numeric)

@task()
def extract(file_path, output_folder):
    """Extract data from jobs.csv."""
    df = pd.read_csv(file_path)
    context_column = df['context']

    for idx, context_data in enumerate(context_column):
        output_file = os.path.join(output_folder, f"extracted_{idx}.txt")
        save_text_to_file(output_file, str(context_data))

@task()
def transform(input_folder, output_folder):
    """Clean and convert extracted elements to json."""
    transformed_data_list = []

    for filename in os.listdir(input_folder):
        with open(os.path.join(input_folder, filename), 'r') as file:
            context_data = json.load(file)

            # Clean job description
            cleaned_description = clean_description(context_data['job']['description'])
            context_data['job']['description'] = cleaned_description

            # Transform the schema
            transformed_data = transform_schema(context_data)
            transformed_data_list.append(transformed_data)

            output_file = os.path.join(output_folder, f"transformed_{filename}")
            save_json_to_file(output_file, transformed_data)

    return transformed_data_list

def clean_description(description):
    # Your cleaning logic for job description
    soup = BeautifulSoup(description, 'html.parser')
    return soup.get_text()

def transform(input_folder, output_folder):
        """Clean and convert extracted elements to json."""
        transformed_data_list = []

        for filename in os.listdir(input_folder):
            with open(os.path.join(input_folder, filename), 'r') as file:
                context_data = json.load(file)

                # Clean job description
                cleaned_description = clean_description(context_data['job']['description'])
                context_data['job']['description'] = cleaned_description

                # Transform the schema
                transformed_data = transform_schema(context_data)
                transformed_data_list.append(transformed_data)

                output_file = os.path.join(output_folder, f"transformed_{filename}")
                save_json_to_file(output_file, transformed_data)

        return transformed_data_list

def clean_description(description):
        # Your cleaning logic for job description
        soup = BeautifulSoup(description, 'html.parser')
        return soup.get_text()

def transform_schema(data):
        transformed_data = {
            "job": {
                "title": data['job']['title'],
                "industry": data['job']['industry'],
                "description": data['job']['description'],
                "employment_type": data['job']['employment_type'],
                "date_posted": data['job']['date_posted'],
            },
            "company": {
                "name": data['company']['name'],
                "link": data['company']['link'],
            },
            "education": {
                "required_credential": data['education']['required_credential'],
            },
            "experience": {
                "months_of_experience": data['experience']['months_of_experience'],
                "seniority_level": data['experience']['seniority_level'],
            },
            "salary": {
                "currency": data['salary']['currency'],
                "min_value": data['salary']['min_value'],
                "max_value": data['salary']['max_value'],
                "unit": data['salary']['unit'],
            },
            "location": {
                "country": data['location']['country'],
                "locality": data['location']['locality'],
                "region": data['location']['region'],
                "postal_code": data['location']['postal_code'],
                "street_address": data['location']['street_address'],
                "latitude": data['location']['latitude'],
                "longitude": data['location']['longitude'],
            },
        }
        return transformed_data


@task()
def load(input_folder, database_path):
    """Load data to sqlite database."""
    engine = create_engine(f'sqlite:///{database_path}')
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    with Session() as session:
        for transformed_data in input_folder:
            # Assuming 'transformed_data' follows the structure of your transformed data
            job = Job(
                title=transformed_data['job']['title'],
                industry=transformed_data['job']['industry'],
                description=transformed_data['job']['description'],
                employment_type=transformed_data['job']['employment_type'],
                date_posted=transformed_data['job']['date_posted']
            )
            session.add(job)

            # Assuming there's a one-to-one relationship between Job and Company
            company = Company(
                job_id=job.id,
                name=transformed_data['company']['name'],
                link=transformed_data['company']['link']
            )
            session.add(company)

            # Add logic for other tables (Education, Experience, Salary, Location)
            education = Education(
                job_id=job.id,
                required_credential=transformed_data['education']['required_credential']
            )
            session.add(education)

            experience = Experience(
                job_id=job.id,
                months_of_experience=transformed_data['experience']['months_of_experience'],
                seniority_level=transformed_data['experience']['seniority_level']
            )
            session.add(experience)

            salary = Salary(
                job_id=job.id,
                currency=transformed_data['salary']['currency'],
                min_value=transformed_data['salary']['min_value'],
                max_value=transformed_data['salary']['max_value'],
                unit=transformed_data['salary']['unit']
            )
            session.add(salary)

            location = Location(
                job_id=job.id,
                country=transformed_data['location']['country'],
                locality=transformed_data['location']['locality'],
                region=transformed_data['location']['region'],
                postal_code=transformed_data['location']['postal_code'],
                street_address=transformed_data['location']['street_address'],
                latitude=transformed_data['location']['latitude'],
                longitude=transformed_data['location']['longitude']
            )
            session.add(location)

        session.commit()

def save_text_to_file(file_path, text):
    with open(file_path, 'w') as file:
        file.write(text)

def save_json_to_file(file_path, data):
    with open(file_path, 'w') as file:
        json.dump(data, file, indent=2)

DAG_DEFAULT_ARGS = {
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=15)
}

TABLES_CREATION_QUERY = """
CREATE TABLE IF NOT EXISTS job (
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
);
"""

@dag(
    dag_id="etl_dag",
    description="ETL LinkedIn job posts",
    tags=["etl"],
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 2),
    catchup=False,
    default_args=DAG_DEFAULT_ARGS
)
def etl_dag():
    """ETL pipeline"""

    create_tables = SqliteOperator(
        task_id="create_tables",
        sqlite_conn_id="sqlite_default",
        sql=TABLES_CREATION_QUERY
    )

    extract_data = extract('source/jobs.csv', 'staging/extracted')
    transform_data = transform('staging/extracted', 'staging/transformed')
    load_data = load(transform_data, 'jobs.db')

    create_tables >> extract_data
    extract_data >> transform_data
    transform_data >> load_data

etl_dag()
