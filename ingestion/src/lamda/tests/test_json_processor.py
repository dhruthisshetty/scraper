import json
import os
import sys
import pytest
from unittest.mock import patch, MagicMock

# Add the parent directory to sys.path to import modules properly
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from process.json_processor import process_employee_data, transform_employee_data

@pytest.fixture
def scraper_config():
    # Based on your folder structure: src/json_100_100.json and src/lamda/tests/test_json_processor.py
    # Need to go up two directories from the test file to reach src, then find the JSON file
    json_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'json_100_100.json'))
    
    return {
        "scraper_id": "100",
        "scraper_name": "json_100",
        "json_file_path": json_path
    }

@pytest.fixture
def sample_raw_data(scraper_config):
    try:
        with open(scraper_config["json_file_path"], 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        pytest.fail(f"Test JSON file not found at {scraper_config['json_file_path']}")
    except json.JSONDecodeError:
        pytest.fail(f"Invalid JSON format in the file at {scraper_config['json_file_path']}")

def test_json_file_loading(sample_raw_data):
    # Verify that the JSON file was loaded successfully
    assert sample_raw_data is not None
    assert "data" in sample_raw_data  # Changed from "employees" to "data"

def test_validate_file_type_and_format(sample_raw_data):
    # Transform the raw data
    transformed_data = transform_employee_data(sample_raw_data)
    
    # Check that the transformed data is a list
    assert isinstance(transformed_data, list)
    
    # Check that each transformed employee has the required fields
    for employee in transformed_data:
        assert isinstance(employee, dict)
        assert "employee_id" in employee
        assert "full_name" in employee
        assert "email" in employee
        assert "phone" in employee
        assert "designation" in employee

def test_data_transformation(sample_raw_data):
    # Transform the raw data
    transformed_data = transform_employee_data(sample_raw_data)
    
    # Print the first few transformed employees for debugging
    print("First few transformed employees:")
    for emp in transformed_data[:3]:
        print(emp)
    
    # Basic validation of transformation
    assert len(transformed_data) > 0
    
    # Example specific validation (adjust based on your actual data)
    first_employee = transformed_data[0]
    assert first_employee["employee_id"] is not None
    assert first_employee["full_name"] is not None
    assert first_employee["designation"] is not None

def test_handle_missing_invalid_data():
    # Specific test for handling incomplete or missing data
    incomplete_data = {
        "data": [  # Changed from "employees" to "data"
            {
                "id": 999,  # A unique ID to ensure it's not in the original data
                "first_name": "Partial",
                "email": "partial.data@example.com"
                # Deliberately missing most fields
            }
        ]
    }
    
    # Transform the incomplete data
    transformed_data = transform_employee_data(incomplete_data)
    
    # Check that the record was processed with default values
    assert len(transformed_data) == 1
    assert transformed_data[0]["full_name"] == "Partial"
    assert transformed_data[0]["years_of_experience"] == 0
    assert transformed_data[0]["designation"] == "system engineer"
@patch('requests.get')
def test_json_file_download(mock_get, scraper_config):
    # Create a mock response
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"data": [{"id": 1, "first_name": "Test", "last_name": "User"}]}
    mock_get.return_value = mock_response
    
    # Assuming you have a function to download JSON data
    from process.json_processor import process_employee_data
    
    # Test the download function
    result = process_employee_data(scraper_config)
    
    # Verify the download was successful
    assert result is not None
    assert "data" in result
    assert len(result["data"]) > 0
