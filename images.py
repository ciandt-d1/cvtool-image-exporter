import logging
import os

import cvtool_images_client
from cvtool_images_client.rest import ApiException

# create an instance of the API class
cvtool_images_client.configuration.host = os.environ.get('IMAGES_API_HOST', 'https://kingpick-dev.scanvas.me/v1')
cvtool_images_client.configuration.debug = os.environ.get('DEBUG', None) is not None
api_instance = cvtool_images_client.ImageApi()


def count(tenant_id, project_id):
    try:
        api_response = api_instance.list_all(tenant_id, project_id, offset=0, limit=1)
        return api_response.meta.total
    except ApiException as e:
        logging.error("Exception when calling ImageApi->list_all: %s\n" % e)
        raise e


def get(tenant_id, project_id, offset, limit):
    try:
        api_response = api_instance.list_all(tenant_id, project_id, offset=offset, limit=limit)
        return api_response
    except ApiException as e:
        logging.error("Exception when calling ImageApi->list_all: %s\n" % e)
        raise e
