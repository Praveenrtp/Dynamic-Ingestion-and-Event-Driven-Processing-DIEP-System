try:
    import datetime, os, sys, json, boto3, uuid, time
    from datetime import datetime
    import pynamodb.attributes as at
    from pynamodb.models import Model
    from pynamodb.attributes import *
    from pynamodb.models import Model
except Exception as e:
    print(e)



global DEV_ACCESS_KEY, DEV_SECRET_KEY, DEV_REGION, DYNAMODB_TABLE_NAME
DEV_REGION = 'us-east-1'
DYNAMODB_TABLE_NAME = os.getenv("DYNAMODB_TABLE")
DEV_ACCESS_KEY = os.getenv("DEV_ACCESS_KEY")
DEV_SECRET_KEY = os.getenv("DEV_AWS_SECRET_KEY")


class GLueJobsMetaData(Model):
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


def lambda_handler(event, context):
    table_name = event.get("table_name")
    job_name = event.get("job_name")

    found = False
    payload = {}

    for item in GLueJobsMetaData.scan():
        print("*", item.job_name)
        if item.table_name == table_name and job_name == item.job_name:
            found = True
            payload = item.glue_payload
            sqs = item.sqs

    print("found", found)

    if found:
        json_glue_payload = json.loads(payload)
        sqs_payload = json.loads(sqs)

        fire_payload = {}
        for key, value in json_glue_payload.items(): fire_payload[f"--{key}"] = value
        for key, value in sqs_payload.items(): fire_payload[f"--{key}"] = value

        print(sqs_payload)

        glue = boto3.client("glue")

        response = glue.start_job_run(
            JobName=job_name,
            Arguments=fire_payload
        )
        print(response)

        return {
            'statusCode': 200,
            'body': json.dumps('Job FIred')
        }


