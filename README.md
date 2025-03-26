# Scraper Project

## Introduction
Title: Scraping Employee Data from API
The Scraper Project is designed to extract employee data from an API endpoint for ingestion into a data warehouse for further analysis.

## Features
- **Data Retrieval**: Fetches employee data from the API endpoint.
- **Data Parsing**: Extracts key fields from JSON response.
- **Data Normalization**: Processes data to match the required schema.
- **Error Handling**: Logs errors and handles API failures gracefully.

## Installation
To set up the scraper locally, follow these steps:

1. **Clone the repository**:
   ```bash
   git clone https://github.com/dhruthisshetty/scraper.git
   ```
2. **Navigate to the project directory**:
   ```bash
   cd ingestion
   cd src
   ```
3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

## Usage
To run the scraper:

1. **Execute the script**:
   ```bash
   python main.py
   ```
2. **Extracted data** will be saved in the `src` directory.

## Data Processing
1. **Normalization Rules**:
   - Create a new column `designation`:
     - `<3 years`: System Engineer
     - `3-5 years`: Data Engineer
     - `5-10 years`: Senior Data Engineer
     - `10+ years`: Lead
   - Combine `first_name` and `last_name` into `Full Name`.
   - Mark phone numbers containing 'x' as "Invalid Number".
   
2. **Data Schema**:
   - Full Name: `string`
   - Email: `string`
   - Phone: `int`
   - Gender: `string`
   - Age: `int`
   - Job Title: `string`
   - Years of Experience: `int`
   - Salary: `int`
   - Department: `string`

## Error Handling
- Logs errors for non-200 API responses.
- Retries failed API requests a limited number of times.
- Handles timeout errors gracefully.

## Test Cases
1. **Verify JSON File Download**
2. **Verify JSON File Extraction**
3. **Validate File Type and Format**
4. **Validate Data Structure**
5. **Handle Missing or Invalid Data**


**Execute the test**:
   ```bash
   python lamda/tests/test_json_processor.py
   ```


