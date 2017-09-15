from time import sleep

import requests
import json
import re

import settings


def get_YARN_apps(app_regex):
    '''
    get target yarn apps
    :param app_regex:
    :return: (app_name, track_url)
    '''
    url = settings.YARN + '?states=RUNNING'
    html_doc = requests.get(url).content
    result = json.loads(html_doc)
    for app in result['apps']['app']:
        if re.match(app_regex, app['name']):
            yield (app['name'], app['trackingUrl'] + 'metrics/json')

def get_target_apps(app_names):
    '''
    get target yarn apps
    :param app_regex:
    :return: (app_name, track_url)
    '''
    url = settings.YARN + '?states=RUNNING'
    html_doc = requests.get(url).content
    result = json.loads(html_doc)
    for app in result['apps']['app']:
        if app['name'] in app_names:
            yield (app['name'], app['trackingUrl'] + 'metrics/json')