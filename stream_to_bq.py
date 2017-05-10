import argparse
import logging
import sys
from pprint import pprint

from google.cloud import bigquery
from google.cloud.logging.handlers.container_engine import ContainerEngineHandler

from bq_data_loader import SCHEMA
from images import count, get

logging.getLogger().setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)
ch = ContainerEngineHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("tenant_id", help="tenant_id")
    parser.add_argument("project_id", help="project_id")
    parser.add_argument("gcp_project", help="Google Cloud Platform project_id")
    args = parser.parse_args()

    tenant_id = args.tenant_id
    project_id = args.project_id
    offset = 0
    limit = 100

    logger.info('Starting export for tenant: %s, project: %s', tenant_id, project_id)

    total = count(tenant_id, project_id)

    logger.info('%s image(s) to export', total)

    while offset < total:
        logger.debug('About to fetch image batch: offset: %s, limit: %s', offset, limit)
        api_response = get(tenant_id, project_id, offset=offset, limit=limit)
        items = api_response.items
        logger.debug('Got batch with %s image(s)', len(items))
        images = []

        for image in items:
            image_to_dict = image.to_dict()
            exif_annotations_dict = image_to_dict.pop('exif_annotations', dict())

            del image_to_dict['annotations']
            del image_to_dict['vision_annotations']

            if exif_annotations_dict:
                image_to_dict['exif_annotations'] = [dict(key=k, value=v) for k, v in exif_annotations_dict.items()]
            images.append(image_to_dict)

        stream_data(args.gcp_project, tenant_id, 'image', images)
        offset += limit

    logger.info('Images exported and stored')


def stream_data(gcp_project, dataset_name, table_name, rows):
    bigquery_client = bigquery.Client(gcp_project)

    dataset = bigquery_client.dataset(dataset_name)
    if not dataset.exists():
        dataset.create()

    bq_table = dataset.table('images', SCHEMA)
    if not bq_table.exists():
        bq_table.create()

    # Reload the table to get the schema.
    bq_table.reload()

    # https://googlecloudplatform.github.io/google-cloud-python/stable/bigquery-table.html#google.cloud.bigquery.table.Table.insert_data
    errors = bq_table.insert_data(rows)

    if not errors:
        print('Loaded 1 row into {}:{}'.format(dataset_name, table_name))
    else:
        print('Errors:')
        pprint(errors)


if __name__ == '__main__':
    main()
