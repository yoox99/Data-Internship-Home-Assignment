# tests/test_etl.py
import os
import json
import html
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from datetime import datetime
import pytest
import sys

sys.path.append(r'C:/Users/Youcode/Desktop/Data-Internship-Home-Assignment-main/dags')
from yasserETL import clean_description,extract, transform, load


class TestETL:

    @pytest.mark.parametrize("description", ["<p>This is a test<br>description</p>", "<br>Another<br>test<br>"])
    def test_clean_description(self, description):
        cleaned_description = clean_description(description)
        assert '<' not in cleaned_description
        assert '>' not in cleaned_description
        assert 'br' not in cleaned_description

    def test_transformed_data_format(self):
        # Run the extract and transform functions
        extract_result = extract()
        transform_result = transform()

        for transformed_item in transform_result:
            # Assertions for cleaned description
            job_description = transformed_item['job']['description']
            assert '<' not in job_description
            assert '>' not in job_description
            assert 'br' not in job_description

            # Assertions for data format in the transformation
            assert isinstance(transformed_item, dict)

            # Check if the expected keys are present in the transformed data
            expected_keys = ['job', 'company', 'education', 'experience', 'salary', 'location']
            assert all(key in transformed_item for key in expected_keys)

            # Check data format within 'job'
            job_data = transformed_item['job']
            assert isinstance(job_data, dict)
            assert all(isinstance(job_data.get(key), str) for key in ['title', 'industry', 'description', 'employment_type', 'date_posted'])

            # Check data format within 'company'
            company_data = transformed_item['company']
            assert isinstance(company_data, dict)
            assert all(isinstance(company_data.get(key), str) for key in ['name', 'link'])

            # Check data format within 'education'
            education_data = transformed_item['education']
            assert isinstance(education_data, dict)
            assert isinstance(education_data.get('required_credential'), str)

            # Check data format within 'experience'
            experience_data = transformed_item['experience']
            assert isinstance(experience_data, dict)
            assert all(isinstance(experience_data.get(key), (int, str)) for key in ['months_of_experience', 'seniority_level'])

            # Check data format within 'salary'
            salary_data = transformed_item['salary']
            assert isinstance(salary_data, dict)
            assert all(isinstance(salary_data.get(key), (str, int)) for key in ['currency', 'min_value', 'max_value', 'unit'])

            # Check data format within 'location'
            location_data = transformed_item['location']
            assert isinstance(location_data, dict)
            assert all(isinstance(location_data.get(key), (str, int, float)) for key in ['country', 'locality', 'region', 'postal_code', 'street_address', 'latitude', 'longitude'])
