import os
import json
from datetime import datetime, timedelta
from etl_dag import extract, transform, load, clean_description, transform_schema

def test_extract(tmpdir):
    # Create a temporary directory for test files
    test_folder = tmpdir.mkdir("tests")
    test_file = test_folder.join("jobs.csv")
    test_file.write("context\n{\"job\": {\"title\": \"Test Job\"}}")

    # Call the extract function
    extract(test_file, test_folder)

    # Check if the extracted file is created
    extracted_file = test_folder.join("extracted.txt")
    assert extracted_file.exists()

def test_clean_description():
    # Test the clean_description function
    dirty_description = "<p>Dirty description</p>"
    cleaned_description = clean_description(dirty_description)
    assert cleaned_description == "Dirty description"

def test_transform_schema():
    # Test the transform_schema function
    input_data = {
        "job": {"title": "Test Job", "industry": "Test Industry"},
        "company": {"name": "Test Company", "link": "https://test.com"},
        "education": {"required_credential": "Test Credential"},
        "experience": {"months_of_experience": 12, "seniority_level": "Mid"},
        "salary": {"currency": "USD", "min_value": 50000, "max_value": 70000, "unit": "year"},
        "location": {"country": "Test Country", "locality": "Test Locality", "region": "Test Region",
                     "postal_code": "12345", "street_address": "Test Street", "latitude": 1.23, "longitude": 4.56}
    }

    expected_output = {
        "job": {"title": "Test Job", "industry": "Test Industry"},
        "company": {"name": "Test Company", "link": "https://test.com"},
        "education": {"required_credential": "Test Credential"},
        "experience": {"months_of_experience": 12, "seniority_level": "Mid"},
        "salary": {"currency": "USD", "min_value": 50000, "max_value": 70000, "unit": "year"},
        "location": {"country": "Test Country", "locality": "Test Locality", "region": "Test Region",
                     "postal_code": "12345", "street_address": "Test Street", "latitude": 1.23, "longitude": 4.56}
    }

    assert transform_schema(input_data) == expected_output

def test_etl_dag(tmpdir):
    # Test the entire ETL DAG
    # Create temporary directories for input and output
    source_folder = tmpdir.mkdir("source")
    staging_folder = tmpdir.mkdir("staging")

    # Create a temporary jobs.csv file
    test_file = source_folder.join("jobs.csv")
    test_file.write("context\n{\"job\": {\"title\": \"Test Job\"}}")

    # Run the entire ETL DAG
    extract(test_file, staging_folder.join("extracted"))
    transformed_data = transform(staging_folder.join("extracted"), staging_folder.join("transformed"))
    load(transformed_data, staging_folder.join("jobs.db"))

    # Check if the transformed file is created
    transformed_file = staging_folder.join("transformed", "transformed.txt")
    assert transformed_file.exists()
