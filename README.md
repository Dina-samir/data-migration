# Data Migration Project using Apache Airflow

This project sets up a data migration pipeline using Apache Airflow, PostgreSQL, and MySQL. The pipeline is designed to synchronize data from a PostgreSQL database to a MySQL database. The Airflow DAG provides the ability to either append new data or overwrite existing tables in the MySQL database.

## Table of Contents

- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Directory Structure](#directory-structure)
- [Setup Instructions](#setup-instructions)
  - [1. Download Docker Compose YAML](#1-download-docker-compose-yaml)
  - [2. Create Environment Variables](#2-create-environment-variables)
  - [3. Edit docker-compose.yaml](#3-edit-docker-composeyaml)
  - [4. Initialize and Start Services](#4-initialize-and-start-services)
  - [5. Access Airflow Web UI](#5-access-airflow-web-ui)
- [Airflow DAG: migrate_data_dag](#airflow-dag-migrate_data_dag)
  - [Description](#description)
  - [DAG Structure](#dag-structure)
  - [Key Functions](#key-functions)
  - [Running the DAG](#running-the-dag)
- [Customization](#customization)
- [Troubleshooting](#troubleshooting)
- [License](#license)




## Architecture

The project uses Docker Compose to orchestrate the following services:

- **Postgres**: The source database containing the data to be migrated.
- **MySQL**: The destination database where data is migrated.
- **Airflow Webserver**: The web-based interface to monitor and manage DAGs.
- **Airflow Scheduler**: Schedules and triggers DAGs.
- **Airflow Init**: Initializes the Airflow metadata database.

## Prerequisites

Ensure you have the following installed:

- Docker: [Install Docker](https://docs.docker.com/get-docker/)
- Docker Compose: [Install Docker Compose](https://docs.docker.com/compose/install/)

## Directory Structure

- dags - migrate_data_dag.py # The Airflow DAG for migrating data
- logs                       # Directory for Airflow logs
- plugins                    # Directory for custom Airflow plugins
- data_mysql                 # MySQL data directory
- init.sql                   # SQL script to initialize Postgres
- source.sql                 # SQL script to populate the Postgres database
- docker-compose.yaml        # Docker Compose configuration file

## Setup Instructions

### 1: Download Docker Compose YAML
download the Airflow Docker Compose configuration:

    bash: curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.0.2/docker-compose.yaml'

### 2: Create Environment Variables
Create a .env file to define user IDs for the Docker containers:

    bash: echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env

### 3: Edit docker-compose.yaml
The docker-compose.yaml has been customized to include:

- A PostgreSQL: service with initialization scripts.
- A MySQL: service for the migration target.

### 4: Initialize and Start Services

    bash: docker-compose up -d

### 5: Access Airflow Web UI
The Airflow web server will be accessible at http://localhost:8090.



## Airflow DAG: migrate_data_dag
### Description

The migrate_data_dag is designed to migrate data from PostgreSQL to MySQL. It supports two types of data migration:

    Append: Only new or updated records are migrated.
    Overwrite: All data in the target table is overwritten with the latest data from the source table.

### DAG Structure

#### Branch Task: 
Decides whether to append data or overwrite existing tables.
#### Create MySQL Tables: 
Drops and recreates tables in MySQL based on the PostgreSQL schema.
#### Migrate Data: 
Transfers data from PostgreSQL to MySQL. For append we used last_update column for tables contain it and select max(id) for tables not contain last_update column
#### Dummy Task: 
A placeholder task for when no table recreation is needed.
#### Data Completeness Task: 
Checks that the number of records in the PostgreSQL tables matches the number of records in the corresponding MySQL tables after migration.
                       

### Key Functions

  - map_postgres_to_mysql(pg_type): Maps PostgreSQL data types to MySQL equivalents.
  - convert_lists_to_string(row): Converts lists in data to JSON strings.
  - create_mysql_tables(**kwargs): Creates MySQL tables based on the schema from PostgreSQL.
  - migrate_all_tables(**kwargs): Migrates data from PostgreSQL to MySQL, either appending or overwriting data based on the DAG configuration.
  - decide_branch(**kwargs): Determines whether to run the table creation task or skip it.

### Running the DAG

The DAG is scheduled to run daily at midnight (@daily). You can manually trigger it through the Airflow web interface or CLI if needed.
Configuration

You can modify the migration type by passing a parameter to the DAG run:

    Overwrite: {"type": "overwrite"}
    Append: {"type": "append"}

Example Trigger Command
To trigger the DAG with an overwrite option, you can use the following Airflow CLI command:

    bash: airflow dags trigger migrate_data_dag -c '{"type":"overwrite"}'



