try:
    import datetime, os, sys, json, boto3, uuid, time, re
    from pynamodb.models import Model
    from pynamodb.attributes import UnicodeAttribute, MapAttribute
    from datetime import datetime

    import pynamodb.attributes as at
    from pynamodb.models import Model
    from pynamodb.attributes import *
    from dotenv import load_dotenv

    print("All imports are ok............")
except Exception as e:
    print("Error: {} ".format(e))

load_dotenv("../infrasture/.env")

global DEV_ACCESS_KEY, DEV_SECRET_KEY, DEV_REGION, DYNAMODB_TABLE_NAME
DEV_REGION = 'us-east-1'
DYNAMODB_TABLE_NAME = os.getenv("DYNAMODB_TABLE")
DEV_ACCESS_KEY = os.getenv("DEV_ACCESS_KEY")
DEV_SECRET_KEY = os.getenv("DEV_AWS_SECRET_KEY")


# ==================================================
# INFRA ON FLY
# ==================================================

def create_sqs_queue(client, queue_name):
    try:
        response = client.create_queue(QueueName=queue_name)
        return response["QueueUrl"]
    except client.exceptions.QueueNameExists:
        print(f"Queue '{queue_name}' already exists.")
        return client.get_queue_url(QueueName=queue_name)["QueueUrl"]


def configure_sqs_policy(client, queue_arn, queue_url, bucket, aws_account_id):
    sqs_policy = {
        "Version": "2012-10-17",
        "Id": "example-ID",
        "Statement": [
            {
                "Sid": "example-statement-ID",
                "Effect": "Allow",
                "Principal": {
                    "Service": "s3.amazonaws.com"
                },
                "Action": "SQS:SendMessage",
                "Resource": queue_arn,
                "Condition": {
                    "StringEquals": {
                        "aws:SourceAccount": aws_account_id
                    },
                    "ArnLike": {
                        "aws:SourceArn": f"arn:aws:s3:*:*:{bucket}"

                    }
                }
            }
        ]
    }
    client.set_queue_attributes(QueueUrl=queue_url, Attributes={"Policy": json.dumps(sqs_policy)})


def configure_s3_event(client, bucket_name, queue_arn, prefix, s3_event_name):
    if prefix:
        s3_event_configurations = [
            {
                "Id": s3_event_name,
                "Events": ["s3:ObjectCreated:*"],
                "QueueArn": queue_arn,
                "Filter": {
                    "Key": {
                        "FilterRules": [
                            {
                                "Name": "prefix",
                                "Value": prefix
                            }
                        ]
                    }
                }
            }
        ]

        client.put_bucket_notification_configuration(
            Bucket=bucket_name,
            NotificationConfiguration={"QueueConfigurations": s3_event_configurations}
        )
    else:
        s3_event_configurations = [
            {
                "Id": s3_event_name,
                "Events": ["s3:ObjectCreated:*"],
                "QueueArn": queue_arn

            }
        ]

        client.put_bucket_notification_configuration(
            Bucket=bucket_name,
            NotificationConfiguration={"QueueConfigurations": s3_event_configurations}
        )


def extract_bucket_name(s3_ingestion_path):
    match = re.match(r"s3://([^/]+)/", s3_ingestion_path)
    if match:
        return match.group(1)
    else:
        raise ValueError("Invalid S3 ingestion path")


def create_dlq(client, queue_name):
    dlq_name = f"{queue_name}-dlq"
    try:
        response = client.create_queue(QueueName=dlq_name)
        return response["QueueUrl"]
    except client.exceptions.QueueNameExists:
        print(f"DLQ '{dlq_name}' already exists.")
        return client.get_queue_url(QueueName=dlq_name)["QueueUrl"]


def configure_dlq_policy(client, dlq_arn, dlq_queue_url, aws_account_id):
    dlq_policy = {
        "Version": "2012-10-17",
        "Id": "example-dlq-ID",
        "Statement": [
            {
                "Sid": "example-dlq-statement-ID",
                "Effect": "Allow",
                "Principal": {
                    "Service": "sqs.amazonaws.com"
                },
                "Action": "SQS:SendMessage",
                "Resource": dlq_arn,
                "Condition": {
                    "StringEquals": {
                        "aws:SourceAccount": aws_account_id
                    },
                    "ArnLike": {
                        "aws:SourceArn": dlq_arn
                    }
                }
            }
        ]
    }
    client.set_queue_attributes(QueueUrl=dlq_queue_url, Attributes={"Policy": json.dumps(dlq_policy)})


def create_sqs_dlq_and_configure_s3_events(json_payload):
    try:
        # ==========PAYLOAD ===============
        queue_config = {}

        aws_access_key = DEV_ACCESS_KEY
        aws_secret_key = DEV_SECRET_KEY
        aws_region = DEV_REGION
        print("DEV_REGION", DEV_REGION)

        json_payload['sqs_queue_name'] = f"{json_payload.get('table_name')}-ingestion-queue"
        json_payload['bucket'] = extract_bucket_name(json_payload['s3_ingestion_path'])
        json_payload[
            's3_event_name'] = f"{json_payload.get('table_name')}-event-forward-to--{json_payload.get('sqs_queue_name')}"
        print(json.dumps(json_payload, indent=3))

        # ==========Creating Clients ===============
        sqs_client = boto3.client('sqs', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key,
                                  region_name=aws_region)
        s3_client = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key,
                                 region_name=aws_region)
        # =====================================================

        sqs_queue_name = json_payload.get("sqs_queue_name")
        bucket = json_payload.get("bucket")

        # Create the main SQS queue
        queue_url = create_sqs_queue(sqs_client, sqs_queue_name)
        queue_arn = f"arn:aws:sqs:{aws_region}:{json_payload['aws_account_id']}:{sqs_queue_name}"
        configure_sqs_policy(sqs_client, queue_arn, queue_url, bucket, json_payload["aws_account_id"])
        queue_config['QUEUE_ARN'] = queue_arn
        queue_config['QUEUE_URL'] = queue_url

        print(f"Queue URL: {queue_url}")
        print(f"Queue ARN: {queue_arn}")

        # Create and configure the Dead Letter Queue (DLQ)
        dlq_queue_url = create_dlq(sqs_client, sqs_queue_name)
        dlq_queue_arn = f"arn:aws:sqs:{aws_region}:{json_payload['aws_account_id']}:{sqs_queue_name}-dlq"
        queue_config['DLQ_QUEUE_ARN'] = dlq_queue_arn
        queue_config['DLQ_QUEUE_URL'] = dlq_queue_url

        dlq_redrive_policy = {
            "deadLetterTargetArn": dlq_queue_arn,
            "maxReceiveCount": 1  # Adjust this as needed
        }
        sqs_client.set_queue_attributes(
            QueueUrl=queue_url,
            Attributes={
                "RedrivePolicy": json.dumps(dlq_redrive_policy)
            }
        )

        print(f"DLQ Queue URL: {dlq_queue_url}")
        print(f"DLQ Queue ARN: {dlq_queue_arn}")

        configure_s3_event(s3_client, json_payload["bucket"], queue_arn, json_payload.get("prefix"),
                           json_payload.get("s3_event_name"))
        time.sleep(1)
        return queue_config
    except Exception as e:
        print("Error creating INFRA ", e)
        return {}


# ===========DYNAMODB CLASS====================================

class GlueJobsMetaData(Model):
    class Meta:
        table_name = DYNAMODB_TABLE_NAME
        aws_access_key_id = DEV_ACCESS_KEY
        aws_secret_access_key = DEV_SECRET_KEY
        region = DEV_REGION

    job_name = UnicodeAttribute(hash_key=True)
    table_name = UnicodeAttribute(range_key=True)

    active = UnicodeAttribute(null=True)
    created_at = UnicodeAttribute(null=True)
    created_by = UnicodeAttribute(null=True)
    cron_schedule = UnicodeAttribute(null=True)
    s3_ingestion_config = UnicodeAttribute(null=True)
    sqs = UnicodeAttribute(null=True)
    glue_payload = UnicodeAttribute(null=True)


def create_dynamodb_table():
    try:
        GlueJobsMetaData.create_table(billing_mode='PAY_PER_REQUEST')
    except Exception as e:
        pass


# ==================================================
# EVENT BRIDGE DYNAMIC SCHEDULER
# ==================================================
class EventBridge(object):

    def __init__(self, instance=None):
        self.instance = instance
        self.client = boto3.client("events",
                                   aws_access_key_id=DEV_ACCESS_KEY,
                                   aws_secret_access_key=DEV_SECRET_KEY,
                                   region_name=DEV_REGION)

    def run(self):
        try:

            response = self.client.put_rule(
                Name=self.instance.RuleName,
                ScheduleExpression=self.instance.ScheduleExpression,
                State=self.instance.State,
                Description=self.instance.Description,
            )

            response = self.client.put_targets(
                Rule=self.instance.RuleName,
                Targets=[
                    {
                        'Id': self.instance.Id,
                        'Arn': self.instance.lambdaArn,
                        'Input': json.dumps(self.instance.inputEvent),
                    },
                ],
            )
            return response

        except Exception as e:
            print("error", e)
            return 'error'


class InputTriggers(object):

    def __init__(self, RuleName='', ScheduleExpression='',
                 State='DISABLED', Description='', lambdaArn='',
                 inputEvent={}, Id='test-id'):
        self.RuleName = RuleName
        self.ScheduleExpression = "cron({})".format(ScheduleExpression)
        self.State = State
        self.Description = Description
        self.lambdaArn = lambdaArn
        self.inputEvent = inputEvent
        self.Id = Id


class GlueIngestionFramework:

    def __init__(self):
        self.glue = boto3.client(
            "glue",
            aws_access_key_id=DEV_ACCESS_KEY,
            aws_secret_access_key=DEV_SECRET_KEY,
            region_name=DEV_REGION,
        )

    def create_glue_ingestion_job_with_cron(self,
                                            job_name,
                                            table_name,
                                            active="False",
                                            created_by='datateam',
                                            cron_schedule='0/15 * * * ? *',
                                            lambdaArn="",
                                            glue_payload={},
                                            s3_ingestion_config={}
                                            ):

        if s3_ingestion_config == {}:
            error = "S3 Ingestion Config cannot be Null"
            raise (error)

        trigger_payload = {"job_name": job_name, "table_name": table_name}
        state = "DISABLED"
        RuleName = f"Glue_Job_Fire_{table_name}"

        if active == "true" or active == "True":
            state = "ENABLED"

        instance_ = InputTriggers(RuleName=RuleName,
                                  ScheduleExpression=cron_schedule,
                                  lambdaArn=lambdaArn,
                                  State=state,
                                  inputEvent=trigger_payload
                                  )

        instanceEvent = EventBridge(instance=instance_)
        response_event_bridge = instanceEvent.run()
        print("response_event_bridge", response_event_bridge)

        print("Creating infrastructure ")

        queue_config = create_sqs_dlq_and_configure_s3_events(
            json_payload=s3_ingestion_config
        )
        if queue_config == {}:
            error = "Failed to create resources"
            raise (error)

        if response_event_bridge.get("ResponseMetadata").get(
                "HTTPStatusCode").__str__() == "200" and queue_config != {}:
            print("Event Bridge Successful")
            GlueJobsMetaData(
                job_name=job_name,
                table_name=table_name,
                created_at=datetime.now().__str__(),
                active=active,
                created_by=created_by,
                cron_schedule=cron_schedule,
                glue_payload=json.dumps(glue_payload),
                s3_ingestion_config=json.dumps(s3_ingestion_config),
                sqs=json.dumps(queue_config)

            ).save()

            return True

        return False


def main():
    payload = {
        "active": "False",
        "created_by": "soumil",
        "cron_schedule": "0/15 * * * ? *",
        "jobName": "sample-pre-commit-test",
        "lambda_arn": "arn:aws:lambda:us-east-1:043916019468:function:glue-ingestion-framework-dev-lambda",
        "table_name": "customers",
        "s3_ingestion_config": {
            "s3_ingestion_path": "s3://jt-datateam-sandbox-qa-dev/",
            "table_name": "customers",
            "aws_account_id": "043916019468",
            "prefix": "raw/customers/"

        },
        "glue_payload": {
            "JOB_NAME": "glue-job",
            "ENABLE_CLEANER": "True",
            "ENABLE_HIVE_SYNC": "True",
            "ENABLE_PARTITION": "True",
            "GLUE_DATABASE": "hudidb",
            "GLUE_TABLE_NAME": "customers",
            "HUDI_PRECOMB_KEY": "ts",
            "HUDI_RECORD_KEY": "customer_id",
            "HUDI_TABLE_TYPE": "COPY_ON_WRITE",
            "PARTITON_FIELDS": "year,month",
            "TARGET_S3_PATH": "s3://jt-datateam-sandbox-qa-dev/silver/customers/",
            "INDEX_TYPE": "BLOOM",
            "USE_SQL_TRANSFORMER": "True",
            "SQL_TRANSFORMER_QUERY": "SELECT * ,extract(year from ts) as year, extract(month from ts) as month, extract(day from ts) as day FROM temp;"
        }
    }

    helper = GlueIngestionFramework()

    response = helper.create_glue_ingestion_job_with_cron(
        job_name=payload.get("jobName"),
        table_name=payload.get("table_name"),
        lambdaArn=payload.get("lambda_arn"),  # Corrected here
        active=payload.get("active"),
        created_by=payload.get("created_by"),
        cron_schedule=payload.get("cron_schedule"),
        glue_payload=payload.get("glue_payload"),
        s3_ingestion_config=payload.get("s3_ingestion_config")
    )


# create_dynamodb_table()
main()
