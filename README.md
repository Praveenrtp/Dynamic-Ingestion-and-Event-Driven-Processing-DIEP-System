# Dynamic-Ingestion-and-Event-Driven-Processing-DIEP-System



![image](https://github.com/Praveenrtp/Dynamic-Ingestion-and-Event-Driven-Processing-DIEP-System/assets/46226861/8c900bdb-8a03-46cb-a3a1-2d12075aa6c8)

# Dynamic Ingestion and Event-Driven Processing (DIEP) System

## Overview
The DIEP System is a robust AWS cloud-based architecture designed for scalable data ingestion and processing workflows. This README outlines the system architecture, components, and their interactions.

## Architecture
The DIEP System leverages various AWS services to create a seamless workflow for ingesting, transforming, and loading data. The following components constitute the system:

1. **API Gateway**: Entry point for triggering ingestion jobs via the `/create_job/` endpoint.
2. **Amazon DynamoDB**: Stores job metadata upon creation.
3. **AWS Lambda**: Functions for fetching job metadata and firing Glue jobs with parameters.
4. **AWS EventBridge**: Schedules periodic triggers to Lambda for job maintenance.
5. **Manual Trigger**: Allows manual job execution through the `/fire_job/<jobid>` endpoint.
6. **AWS Glue**: Processes ETL jobs using parameters from Lambda functions.
7. **Amazon S3**: Data storage service for source and destination data.
8. **Amazon SQS**: Message queue service for decoupling and scaling applications.
9. **Infrastructure Automation**: Capable of creating infrastructure components dynamically in response to events or data.

## Setup Instructions
To deploy the DIEP System, follow these setup instructions:

### Prerequisites
- AWS account with proper IAM permissions.
- AWS CLI configured on your local machine.
- Knowledge of CloudFormation if you wish to automate resource creation.

### Deployment Steps
1. **API Gateway Setup**:
   - Create a new API using the AWS Management Console.
   - Define the `/create_job/` and `/fire_job/<jobid>` endpoints.

2. **DynamoDB Configuration**:
   - Create a new table to store job metadata with the required schema.

3. **Lambda Functions**:
   - Implement Lambda functions for fetching metadata and starting Glue jobs.
   - Set up the necessary triggers from API Gateway and EventBridge.

4. **AWS Glue Jobs**:
   - Define Glue jobs with appropriate ETL scripts.

5. **S3 Buckets**:
   - Create S3 buckets for source and destination data.
   - Set up event notifications to trigger SQS messages.

6. **SQS Queue**:
   - Create a new SQS queue to receive events from S3.

### Running the System
- To create a new ingestion job, send a POST request to the `/create_job/` endpoint.
- To manually trigger a job, send a POST request to the `/fire_job/<jobid>` endpoint.

## System Operations
- **Monitoring**: Set up CloudWatch to monitor the system operations.
- **Logging**: Enable logging for all components to track execution and errors.
- **Scaling**: Adjust Lambda concurrency and Glue DPUs based on load.

## Security
Ensure all AWS services are configured with the principle of least privilege using IAM roles and policies.

