import argparse
import logging
import sys
import time
import uuid

from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from google.cloud.logging.handlers.container_engine import ContainerEngineHandler

logging.getLogger().setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)
ch = ContainerEngineHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)


SCHEMA = [
    SchemaField('project_id', 'STRING', mode='NULLABLE'),
    SchemaField('id', 'STRING', mode='REQUIRED'),
    SchemaField('version', 'STRING', mode='REQUIRED'),
    SchemaField('job_id', 'STRING', mode='REQUIRED'),
    SchemaField('original_uri', 'STRING', mode='NULLABLE'),
    SchemaField('exif_annotations', 'RECORD', mode='REPEATED', fields=[
        SchemaField('key', 'STRING', mode='REQUIRED'),
        SchemaField('value', 'STRING', mode='REQUIRED')
    ]),

    SchemaField('vision_annotations', 'RECORD', mode='NULLABLE', fields=[
        SchemaField('safeSearchAnnotation', 'RECORD', mode='NULLABLE', fields=[
            SchemaField('adult', 'STRING', mode='NULLABLE'),
            SchemaField('spoof', 'STRING', mode='NULLABLE'),
            SchemaField('medical', 'STRING', mode='NULLABLE'),
            SchemaField('violence', 'STRING', mode='NULLABLE')
        ]),
        SchemaField('labelAnnotations', 'RECORD', mode='REPEATED', fields=[
            SchemaField('mid', 'STRING', mode='NULLABLE'),
            SchemaField('locale', 'STRING', mode='NULLABLE'),
            SchemaField('description', 'STRING', mode='NULLABLE'),
            SchemaField('score', 'FLOAT', mode='NULLABLE'),
            SchemaField('boundingPoly', 'RECORD', mode='REPEATED', fields=[
                SchemaField('x', 'INTEGER', mode='NULLABLE'),
                SchemaField('y', 'INTEGER', mode='NULLABLE')
            ])
        ]),
        SchemaField('landmarkAnnotations', 'RECORD', mode='REPEATED', fields=[
            SchemaField('mid', 'STRING', mode='NULLABLE'),
            SchemaField('locale', 'STRING', mode='NULLABLE'),
            SchemaField('description', 'STRING', mode='NULLABLE'),
            SchemaField('score', 'FLOAT', mode='NULLABLE'),
            SchemaField('boundingPoly', 'RECORD', mode='REPEATED', fields=[
                SchemaField('x', 'INTEGER', mode='NULLABLE'),
                SchemaField('y', 'INTEGER', mode='NULLABLE')
            ])
        ]),
        SchemaField('logoAnnotations', 'RECORD', mode='REPEATED', fields=[
            SchemaField('mid', 'STRING', mode='NULLABLE'),
            SchemaField('locale', 'STRING', mode='NULLABLE'),
            SchemaField('description', 'STRING', mode='NULLABLE'),
            SchemaField('score', 'FLOAT', mode='NULLABLE'),
            SchemaField('boundingPoly', 'RECORD', mode='REPEATED', fields=[
                SchemaField('x', 'INTEGER', mode='NULLABLE'),
                SchemaField('y', 'INTEGER', mode='NULLABLE')
            ])
        ]),
        SchemaField('imagePropertiesAnnotation', 'RECORD', mode='NULLABLE', fields=[
            SchemaField('dominantColors', 'RECORD', mode='NULLABLE', fields=[
                SchemaField('colors', 'RECORD', mode='REPEATED', fields=[
                    SchemaField('pixelFraction', 'FLOAT', mode='NULLABLE'),
                    SchemaField('score', 'FLOAT', mode='NULLABLE'),
                    SchemaField('color', 'RECORD', mode='NULLABLE', fields=[
                        SchemaField('red', 'FLOAT', mode='NULLABLE'),
                        SchemaField('green', 'FLOAT', mode='NULLABLE'),
                        SchemaField('blue', 'FLOAT', mode='NULLABLE'),
                        SchemaField('alpha', 'FLOAT', mode='NULLABLE')
                    ]),
                ])
            ])
        ])
    ])
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("tenant_id", help="tenant_id")
    parser.add_argument("project_id", help="project_id")
    parser.add_argument("source_bucket", help="source bucket")
    parser.add_argument("input_file", help="Input json file containing images")
    parser.add_argument("gcp_project", help="Google Cloud Platform project_id")
    args = parser.parse_args()

    tenant_id = args.tenant_id
    project_id = args.project_id
    input_bucket = args.source_bucket
    input_file = args.input_file

    # logger.info('Starting export for tenant: %s, project: %s', tenant_id, project_id)
    client = bigquery.Client(args.gcp_project)

    dataset = client.dataset(tenant_id)
    if not dataset.exists():
        dataset.create()

    bq_table = dataset.table('images', SCHEMA)
    # TODO add flag to force table recreation
    # if bq_table.exists():
    #     bq_table.delete()
    if not bq_table.exists():
        bq_table.create()

    source_uri = 'gs://{}/image-export/{}/{}/{}'.format(input_bucket, tenant_id, project_id, input_file)

    job = client.load_table_from_storage(str(uuid.uuid4()), bq_table, source_uri)
    job.source_format = 'NEWLINE_DELIMITED_JSON'
    job.write_disposition = 'WRITE_TRUNCATE'

    logger.info('Starting job')
    job.begin()

    retry_count = 100
    while retry_count > 0 and job.state != 'DONE':
        retry_count -= 1
        time.sleep(10)
        logger.info('Reloading %s', job)
        job.reload()  # API call

    logger.info('%s', job)
    logger.info('%s, %s, %s, %s', job.name, job.job_type, job.created, job.state)

if __name__ == '__main__':
    main()
