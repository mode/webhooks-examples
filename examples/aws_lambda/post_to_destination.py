"""
Performs a POST to the specified destination URL. This config is
specific to running on the AWS Lambda service.

The URL is read from the `destination_url` environment variable.

"""
import requests
import json
import hookrich as hr
import logging
import os


log = logging.getLogger()
log.setLevel(logging.INFO)


def _response(**resp):
    """
    Return an API Gateway compatible response.

    """
    return {'body': json.dumps(resp)}


def post_to_destination(event_name, event_url):
    """
    Call the destination URL with event details.

    """
    payload = hr.enrich_payload(event_name, event_url)
    payload['event_name'] = event_name

    return requests.post(os.environ['destination_url'], data=payload).json()


def lambda_function_handler(event, context):
    """
    AWS Lambda entry point.

    """
    log.info("Received payload: {}".format(event))

    try:
        body = json.loads(event['body'])
        event_name = body['event']
    except (TypeError, KeyError):
        msg = "Invalid webhook event: {}".format(event)
        log.error(msg)
        return _response(result='error', message=msg)

    try:
        event_url = body[hr.WEBHOOK_EVENTS[event_name]['url']]
    except KeyError:
        msg = "Unsupported event type: {}".format(event_name)
        log.error(msg)
        return _response(result='error', message=msg)

    try:
        response = post_to_destination(event_name, event_url)
    except Exception as error:
        log.error(str(error))
        return _response(result='error', message=str(error))

    return _response(result='success', response=response)

