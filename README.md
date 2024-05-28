
# Czechoslovakia Banks Data Analysis

## Table of Contents
* Project Description
* Installation
* Usage
* Workflow
* Features
* Data Sources
* Contributing
* License
* Contact
* Project Description
* Overview

## Project Description
### Overview
This project involves the analysis of Czechoslovakia Banks data using various tools including AWS S3, Snowflake, Power BI, and Excel. The primary aim is to clean, structure, and analyze the data to derive meaningful insights and create comprehensive reports.

### Motivation
The goal of this project is to demonstrate the end-to-end data analysis process starting from data cleaning to visualization. By leveraging cloud services and powerful analytical tools, this project showcases how to handle large datasets efficiently and create insightful reports.

### Screenshots
Include a few screenshots or a demo GIF to give a visual overview of the project. (Screenshots to be added)
![Bank_Report_2_Page_1](https://github.com/Vivek-Tomar-9990/Czechoslovakia-Banks-Data-Analysis/assets/115417489/c10f4efe-98d8-4bdd-96fa-e32c85725539)


## Installation
### Prerequisites
* Python 3.8+
* AWS account
* Snowflake account
* Power BI (Desktop and Service)
* Microsoft Excel

Setup
Clone the repository:

bash
Copy code
git clone https://github.com/yourusername/yourproject.git
cd yourproject
Install required dependencies:

bash
Copy code
pip install -r requirements.txt
Setup AWS S3 bucket and Snowflake:
Follow the steps in the Workflow section to set up AWS S3 and Snowflake.

Usage
Basic Usage
Explain how to use the project, focusing on the key steps such as cleaning data, uploading to AWS S3, and visualizing in Power BI.

bash
Copy code
# Example command to run the project (if applicable)
python main.py --input data/inputfile.csv --output results/outputfile.csv

## Workflow
### Step 1: Data Cleaning and Structuring
Cleaned and structured the CSV file in Excel to prepare it for upload.
### Step 2: AWS S3 Setup
Created an AWS S3 bucket, policy, and role to securely store the data.
### Step 3: Snowflake Database Creation
Created a Snowflake database and the necessary tables.
Configured integration and staging in Snowflake.
Set up Snowpipe for automatic incremental data loading.
### Step 4: Connecting Power BI to Snowflake
Connected Power BI to Snowflake to fetch the data.
Performed additional transformations in Power Query if needed.
### Step 5: Report Creation
Created comprehensive reports in Power BI.

## Features
* Data Cleaning: Structured raw data for analysis.
* AWS S3: Secure data storage and management.
* Snowflake: Efficient data loading and processing.
* Power BI: Interactive visualizations and reporting.
* Excel: Initial data preparation and cleaning.

## Data Sources
### Source 1
* Name: Czechoslovakia Banks Data
* Description: Dataset containing information about banks in Czechoslovakia.

## Contact
### Maintainers
* Vivek Tomar - vivektomar9990@gmail.com
### Feedback
For any questions or feedback, feel free to reach out or open an issue on GitHub.
