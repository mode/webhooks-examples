"""
Contextually constructs a Slack message based on the Mode webhook event
and then performs a POST to the specified Slack webhook URL. This config is
specific to running on the AWS Lambda service.

The Slack webhook URL is read from the `slack_webhook_url` environment variable.

"""
import requests
import json
import hookrich as hr
import logging
import os


log = logging.getLogger()
log.setLevel(logging.INFO)


alerts = {
    'report_id': 643059,
    'field': 'total_amt_usd',
    'threshold': 1000
}


def _response(**resp):
    """
    Return an API Gateway compatible response.

    """
    return {'body': json.dumps(resp)}


def definition_created_message(payload):
    """
    Build a Slack notification about a new definition.

    """
    definition_creator = payload['definition']['creator']
    definition_url = payload['definition']['url']
    definition_name = payload['definition']['name']

    message = '{} just created the <{}|{}> definition.'
    message = message.format(definition_creator, definition_url, definition_name)

    attachments = [
        {
            'fallback': message,
            'color': 'good',
            'author_name': 'Mode',
            'author_link': 'https://modeanalytics.com/',
            'title': 'New Definition :plus1:',
            'text': message
        }
    ]

    return attachments


def definition_updated_message(payload):
    """
    Build a Slack notification about an updated definition.

    """
    definition_url = payload['definition']['url']
    definition_name = payload['definition']['name']

    message = 'The <{}|{}> definition was just updated.'
    message = message.format(definition_url, definition_name)

    attachments = [
        {
            'fallback': message,
            'color': 'warning',
            'author_name': 'Mode',
            'author_link': 'https://modeanalytics.com/',
            'title': 'Definition Updated :heavy_exclamation_mark:',
            'text': message
        }
    ]

    return attachments


def member_joined_organization_message(payload):
    """
    Build a Slack message informing about a new member.

    """
    member_name = payload['user']['name']
    member_url = payload['user']['url']
    org_name = payload['organization']['name']
    org_url = payload['organization']['url']

    message = 'Say hi! <{}|{}> just joined the <{}|{}> organization.'
    message = message.format(member_url, member_name, org_url, org_name)

    attachments = [
        {
            'fallback': message,
            'color': 'good',
            'author_name': 'Mode',
            'author_link': 'https://modeanalytics.com/',
            'title': 'New Org Member :wave:',
            'text': message
        }
    ]

    return attachments


def new_database_connection_message(payload):
    """
    Build a Slack notification about a new database connection.

    """
    connection_url = payload['connection']['url']
    connection_name = payload['connection']['name']
    connection_vendor = payload['connection']['vendor']
    connection_provider = payload['connection']['provider']

    message = 'The <{}|{}> data source was just connected.'
    message = message.format(connection_url, connection_name)

    attachments = [
        {
            'fallback': message,
            'color': 'good',
            'author_name': 'Mode',
            'author_link': 'https://modeanalytics.com/',
            'title': 'New Data Source :plus1:',
            'text': message,
            'fields': [
                {
                    'title': 'Vendor',
                    'value': connection_vendor
                },
                {
                    'title': 'Provider',
                    'value': connection_provider
                }
            ]
        }
    ]

    return attachments


def report_created_message(payload):
    """
    Build a Slack message about a newly created report.

    """
    report_url = payload['report']['url']
    report_name = payload['report']['name']
    report_creator = payload['report']['creator']
    space_name = payload['space']['name']
    space_url = payload['space']['url']

    message = '{} just created the <{}|{}> report in the <{}|{}> space.'
    message = message.format(report_creator, report_url, report_name, space_url, space_name)

    attachments = [
        {
            'fallback': message,
            'color': 'good',
            'author_name': 'Mode',
            'author_link': 'https://modeanalytics.com/',
            'title': 'New Report Created :plus1:',
            'text': message
        }
    ]

    return attachments


def report_run_completed_message(payload):
    """
    Build a Slack message for a completed report run.

    """
    report_url = payload['report']['url']
    report_name = payload['report']['name']
    report_run_executor = payload['report_run']['executed_by']
    space_name = payload['space']['name']
    space_url = payload['space']['url']

    if payload['report_run']['state'] == 'succeeded' and payload['report']['id'] == alerts['report_id']:

        for row in payload['report_run']['results']:
            if row[alerts['field']] > alerts['threshold']:

                message = 'Heads up! {} just ran the <{}|{}> report in the <{}|{}> space and it succeeded, but the {} field exceeded the alert threshold.'
                message = message.format(report_run_executor, report_url, report_name, space_url, space_name, alerts['field'])

                attachments = [
                    {
                        'fallback': message,
                        'color': 'warning',
                        'author_name': 'Mode',
                        'author_link': 'https://modeanalytics.com/',
                        'title': 'Threshold Alert :heavy_exclamation_mark:',
                        'text': message,
                        'fields': [
                            {
                                'title': 'Observed Value',
                                'value': row[alerts['field']]
                            },
                            {
                                'title': 'Threshold Value',
                                'value': alerts['threshold']
                            }
                        ]
                    }
                ]
                break

    elif payload['report_run']['state'] == 'succeeded':
        report_run_duration = payload['report_run']['execution_duration']

        message = 'Good news! {} just ran the <{}|{}> report in the <{}|{}> space and it succeeded. It took {} seconds to run.'
        message = message.format(report_run_executor, report_url, report_name, space_url, space_name, report_run_duration)

        attachments = [
            {
                'fallback': message,
                'color': 'good',
                'author_name': 'Mode',
                'author_link': 'https://modeanalytics.com/',
                'title': 'Successful Report Run :success:',
                'text': message
            }
        ]

    elif payload['report_run']['state'] == 'failed':
        report_consecutive_run_failures = payload['report']['consecutive_run_failures']

        message = 'Oh no! {} just ran the <{}|{}> report in the <{}|{}> space and it failed. It has failed the last {} run(s).'
        message = message.format(report_run_executor, report_url, report_name, space_url, space_name, report_consecutive_run_failures)

        attachments = [
            {
                'fallback': message,
                'color': 'danger',
                'author_name': 'Mode',
                'author_link': 'https://modeanalytics.com/',
                'title': 'Failed Report Run :sad-error:',
                'text': message
            }
        ]

    return attachments


def build_slack_message(event_name, payload):
    """
    Build a Slack message from the webhook event.

    """
    if event_name == 'report_run_completed':
        return report_run_completed_message(payload)
    elif event_name == 'report_created':
        return report_created_message(payload)
    elif event_name == 'member_joined_organization':
        return member_joined_organization_message(payload)
    elif event_name == 'definition_created':
        return definition_created_message(payload)
    elif event_name == 'definition_updated':
        return definition_updated_message(payload)
    elif event_name == 'new_database_connection':
        return new_database_connection_message(payload)
    else:
        raise Exception("Unsupported event type: {}".format(event_name))


def post_to_slack(event_name, event_url):
    """
    Post event details to Slack.

    """
    payload = hr.enrich_payload(event_name, event_url)
    payload['event_name'] = event_name

    slack_attachments = build_slack_message(event_name, payload)
    slack_payload = {
        'attachments': slack_attachments,
        'username': 'Mode'
    }

    return requests.post(
               os.environ['slack_webhook_url'],
               json=slack_payload
           ).text


def lambda_function_handler(event, context):
    """
    AWS Lambda entry point.

    """
    log.info('Received payload {}'.format(event))

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
        response = post_to_slack(event_name, event_url)
    except Exception as error:
        log.error(str(error))
        return _response(result='error', message=str(error))

    return _response(result='success', response=response)
