import argparse
import json
import logging
import sys
import tempfile
import uuid

from google.cloud import storage
from google.cloud._helpers import _to_bytes
from google.cloud.logging.handlers.container_engine import ContainerEngineHandler

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
    parser.add_argument("output_bucket", help="Target bucket")
    parser.add_argument("gcp_project", help="Google Cloud Plataform project_id")
    args = parser.parse_args()

    tenant_id = args.tenant_id
    project_id = args.project_id
    offset = 0
    limit = 100
    bucket_name = args.output_bucket

    logger.info('Starting export for tenant: %s, project: %s', tenant_id, project_id)

    storage_client = storage.Client(args.gcp_project)
    bucket = storage_client.get_bucket(bucket_name)
    blob_name = 'image-export/{tenant_id}/{project_id}/{export_id}.json'.format(tenant_id=tenant_id, project_id=project_id, export_id=str(uuid.uuid4()))
    blob = storage.Blob(blob_name, bucket)
    total = count(tenant_id, project_id)

    logger.info('%s image(s) to export', total)

    # gzip_stream = gzip.GzipFile(fileobj=stream, mode='rw')
    # blob.content_encoding = 'gzip'

    # TODO find a way to stream data to GCS instead of buffering in memory
    with tempfile.NamedTemporaryFile() as tmp_file:
        while offset < total:
            logger.debug('About to fetch image batch: offset: %s, limit: %s', offset, limit)
            api_response = get(tenant_id, project_id, offset=offset, limit=limit)
            items = api_response.items
            logger.debug('Got batch with %s image(s)', len(items))
            for image in items:
                logger.debug('Writing image: %s', image.id)

                image_to_dict = image.to_dict()

                del image_to_dict['annotations']
                # del image_to_dict['vision_annotations']

                exif_annotations_dict = image_to_dict.pop('exif_annotations', {})
                if exif_annotations_dict:
                    image_to_dict['exif_annotations'] = [dict(key=k, value=v) for k, v in exif_annotations_dict.items()]

                vision_annotations_dict = image_to_dict.pop('vision_annotations', {})
                if vision_annotations_dict:
                    image_to_dict['vision_annotations'] = json.loads(vision_annotations_dict)

                tmp_file.write(_to_bytes(json.dumps(image_to_dict) + '\n', encoding='utf-8'))
            offset += limit

        content_type = 'application/json'
        ret_val = blob.upload_from_file(tmp_file, content_type=content_type, client=storage_client, rewind=True)
        logger.debug('Temporary file uploaded: %s', ret_val)

    logger.info('Images exported and stored on: %s', blob.path)

if __name__ == '__main__':
    main()
