"""
This module constructs an "enriched" payload by using the URL
supplied by the Mode webhook to call the Mode API and gather relevant
about the event. This "enriched" payload can then be used in a POST to
other services (e.g. Slack, Zapier, Gmail, etc.)

"""
import os.path
import requests
from datetime import datetime


MODE_BASE_URL = 'https://modeanalytics.com/'

WEBHOOK_EVENTS = {
    'report_created': {
        'url': 'report_url',
        'scope': 'report'
    },
    'report_run_started': {
        'url': 'report_run_url',
        'scope': 'report_run'
    },
    'report_run_completed': {
        'url': 'report_run_url',
        'scope': 'report_run'
    },
    'definition_created': {
        'url': 'definition_url',
        'scope': 'definition'
    },
    'definition_updated': {
        'url': 'definition_url',
        'scope': 'definition'
    },
    'new_database_connection': {
        'url': 'connection_url',
        'scope': 'connection'
    },
    'member_joined_organization': {
        'url': 'member_url',
        'scope': 'membership'
    }
}


class EventURL(object):
    """
    Object representation of a Mode event URL.

    """

    def __add__(self, suffix):
        return self.url + suffix

    def __init__(self, url):
        self.url = url
        self.path = url.split(MODE_BASE_URL, 3)[-1]

    def __str__(self):
        return self.url

    @property
    def connection_url(self):
        return self.url.replace('/api/', '/organizations/')

    @property
    def member_token(self):
        return self.path.split('/memberships/')[1].split('?embed[user]')[0]

    @property
    def report_url(self):
        return self.url.split('/runs/')[0]

    @property
    def org(self):
        return self.path.split('/')[1]


def _mode_api_get(endpoint_url):
    """
    Send a GET request to a Mode API endpoint.

    """
    return requests.get(
               endpoint_url,
               auth=(os.environ['api_token'], os.environ['api_password'])
           ).json()


def datetime_iso_convert(iso_string):
    return datetime.strptime(iso_string, '%Y-%m-%dT%H:%M:%S.%fZ')


def report_run_duration(started_at, completed_at):
    """
    Return the duration, in seconds, of a report run.

    """
    report_run_created_at = datetime_iso_convert(started_at)
    report_run_completed_at = datetime_iso_convert(completed_at)

    return (report_run_completed_at - report_run_created_at).seconds


def consecutive_run_failures(url):
    """
    Count the number of consecutive report run failures.

    """
    report_runs_data = get_report_runs(url)
    consecutive_failure_count = 0

    for page in report_runs_data:
        runs = page['_embedded']['report_runs']

        for run in runs:
            if run['state'] == 'succeeded':
                break
        else:
            consecutive_failure_count += len(runs)
            continue

        break

    return consecutive_failure_count


def get_report_runs(url):
    """
    Retrieve report run metadata.

    """
    report_runs_data = _mode_api_get(url + '/runs')

    # Pagination
    total_pages = report_runs_data['pagination']['total_pages']
    max_pagination_pages = 10

    payload = [report_runs_data]

    while report_runs_data['pagination']['page'] < min(total_pages, max_pagination_pages):
        report_runs_data = _mode_api_get(MODE_BASE_URL + report_runs_data['_links']['next_page']['href'])
        payload.append(report_runs_data)

    return payload


def get_report_run_info(url):
    """
    Retrieve the details of a report run.

    """
    report_run_data = _mode_api_get(url)
    results = _mode_api_get(url + '/results/content.json')

    return {
        'report_run': {
            'executed_by': report_run_data['_links']['executed_by']['href'].split('/')[2],
            'account': report_run_data['_links']['account'],
            'share': report_run_data['_links']['share'],
            'report': report_run_data['_links']['report'],
            'query_runs': report_run_data['_links']['query_runs'],
            'python_cell_runs': report_run_data['_links']['python_cell_runs'],
            'state': report_run_data['state'],
            'parameters': report_run_data['parameters'],
            'python_state': report_run_data['python_state'],
            'created_at': report_run_data['created_at'],
            'completed_at': report_run_data['completed_at'],
            'form_fields': report_run_data['form_fields'],
            'execution_duration': report_run_duration(report_run_data['created_at'],
                                                      report_run_data['completed_at']),
            'token': report_run_data['token'],
            'results': results,
            'url': report_run_data['_links']['web_external_url']['href'].split('?')[0]
        }
    }


def get_report_info(url):
    """
    Retrieve the details of a report.

    """
    data = _mode_api_get(url)

    return {
        'report': {
            'name': data['name'],
            'id': data['id'],
            'creator': data['_links']['creator']['href'].split('/')[2],
            'created_at': data['created_at'],
            'edited_at': data['edited_at'],
            'theme_id': data['theme_id'],
            'archived': data['archived'],
            'account_id': data['account_id'],
            'account_username': data['account_username'],
            'full_width': data['full_width'],
            'manual_run_disabled': data['manual_run_disabled'],
            'run_privately': data['run_privately'],
            'is_embedded': data['is_embedded'],
            'is_signed': data['is_signed'],
            'shared': data['shared'],
            'last_successfully_run_at': data['last_successfully_run_at'],
            'consecutive_run_failures': consecutive_run_failures(url),
            'last_successful_run_token': data['last_successful_run_token'],
            'last_run_at': data['last_run_at'],
            'description': data['description'],
            'report_schedules': data['_links']['report_schedules'],
            'report_subscriptions': data['_links']['report_subscriptions'],
            'public': data['public'],
            'account_id': data['account_id'],
            'url': os.path.join(MODE_BASE_URL, data['_links']['self']['href'][5:]),
            'space_token': data['space_token'],
            'web_preview_image': data['web_preview_image']
        }
    }


def get_space_info(url):
    """
    Retrieve details about a space.

    """
    space_data = _mode_api_get(url)

    return {
        'space': {
            'id': space_data['id'],
            'name': space_data['name'],
            'space_type': space_data['space_type'],
            'description': space_data['description'],
            'state': space_data['state'],
            'url': os.path.join(MODE_BASE_URL, space_data['_links']['self']['href'][5:]),
            'restricted': space_data['restricted']
        }
    }


def get_definition_info(url):
    """
    Retrieve details about a definition.

    """
    definition_data = _mode_api_get(url)

    return {
        'definition': {
            'id': definition_data['id'],
            'name': definition_data['name'],
            'created_at': definition_data['created_at'],
            'data_source_id': definition_data['data_source_id'],
            'description': definition_data['description'],
            'source': definition_data['source'],
            'token': definition_data['token'],
            'creator': definition_data['_links']['creator']['href'][5:],
            'url': os.path.join(MODE_BASE_URL, 'editor', url.org, 'definitions', definition_data['token'])
        }
    }


def get_connection_info(url):
    """
    Retrieve data about a connection.

    """
    connection_data = _mode_api_get(url)

    return {
        'connection': {
            'id': connection_data['id'],
            'name' : connection_data['name'],
            'account_id': connection_data['account_id'],
            'account_username': connection_data['account_username'],
            'adapter': connection_data['adapter'],
            'asleep': connection_data['asleep'],
            'bridged': connection_data['bridged'],
            'created_at': connection_data['created_at'],
            'custom_attributes': connection_data['custom_attributes'],
            'database': connection_data['database'],
            'default': connection_data['default'],
            'default_for_organization_id': connection_data['default_for_organization_id'],
            'description': connection_data['description'],
            'display_name': connection_data['display_name'],
            'has_expensive_schema_updates': connection_data['has_expensive_schema_updates'],
            'host': connection_data['host'],
            'ldap': connection_data['ldap'],
            'organization_token': connection_data['organization_token'],
            'port': connection_data['port'],
            'provider': connection_data['provider'],
            'public': connection_data['public'],
            'queryable': connection_data['queryable'],
            'ssl': connection_data['ssl'],
            'token': connection_data['token'],
            'updated_at': connection_data['updated_at'],
            'username': connection_data['username'],
            'vendor': connection_data['vendor'],
            'warehouse': connection_data['warehouse'],
            'url': url.connection_url
        }
    }


def get_org_info(organization):
    """
    Retrieve organization metadata.

    """
    org_data = _mode_api_get(os.path.join(MODE_BASE_URL, 'api', organization))

    return {
        'organization': {
            'id': org_data['id'],
            'name': org_data['name'],
            'token': org_data['token'],
            'user': org_data['user'],
            'username': org_data['username'],
            'plan_code': org_data['plan_code'],
            'private_definition_count': org_data['private_definition_count'],
            'private_definition_limit': org_data['private_definition_limit'],
            'space_count': org_data['space_count'],
            'trial_state': org_data['trial_state'],
            'url': os.path.join(MODE_BASE_URL, organization)
        }
    }


def get_user_info(username):
    """
    Retrieve info about a user.

    """
    user_data = _mode_api_get(os.path.join(MODE_BASE_URL, 'api', username))

    return {
        'user': {
            'email': user_data.get('email', ''),
            'email_verified': user_data.get('email_verified', ''),
            'id': user_data['id'],
            'name': user_data['name'],
            'token': user_data['token'],
            'user': user_data['user'],
            'username': user_data['username'],
            'url': os.path.join(MODE_BASE_URL, username)
        }
    }


def get_membership_info(url):
    """
    Retrieve a membership.

    """
    membership_data = _mode_api_get(os.path.join(MODE_BASE_URL, 'api', url.org, 'memberships', url.member_token))

    links = membership_data['_links']
    organization = links['organization']['href'][5:]
    username = links['user']['href'][5:]
    membership_token = links['self']['href'].split('/memberships/')[1]

    membership_info = {
        'membership': {
            'admin': membership_data['admin'],
            'limited': membership_data['limited'],
            'token': membership_token
        }
    }

    # Grab User Information
    membership_info.update(get_user_info(username))

    # Grab Organization Information
    membership_info.update(get_org_info(organization))

    return membership_info


def enrich_payload(event_name, event_url):
    """
    Use the Mode API to load details about the event.

    """
    event_url = EventURL(event_url)
    scope = WEBHOOK_EVENTS[event_name]['scope']

    if scope == 'report_run':
        #
        # Enrich a report run
        #
        payload = get_report_run_info(event_url)

        # Get Report Information
        payload.update(get_report_info(event_url.report_url))

        # Get Space Information
        space_endpoint_url = os.path.join(MODE_BASE_URL, 'api', event_url.org, 'spaces', payload['report']['space_token'])
        payload.update(get_space_info(space_endpoint_url))

    elif scope == 'report':
        #
        # Enrich a report
        #
        payload = get_report_info(event_url)

        # Get Space Information
        space_endpoint_url = os.path.join(MODE_BASE_URL, 'api', event_url.org, 'spaces', payload['report']['space_token'])
        payload.update(get_space_info(space_endpoint_url))

    elif scope == 'membership':
        #
        # Enrich a membership
        #
        payload = get_membership_info(event_url)

    elif scope == 'connection':
        #
        # Enrich a connection
        #
        payload = get_connection_info(event_url)

    elif scope == 'definition':
        #
        # Enrich a definition
        #
        payload = get_definition_info(event_url)

    return payload
