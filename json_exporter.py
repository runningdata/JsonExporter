import re
from prometheus_client import core as pcore, start_http_server, Metric, REGISTRY
import json
import requests
import sys
import time

import settings
from utils import DBUtils
from utils import YarnUtils

running_cache = dict()


class JsonCollector():
    def init(self, endpoint, appname, srvtype, separator='__'):
        self._endpoint = endpoint
        self._appname = appname
        self._separator = separator
        self._srvtype = srvtype

    def __eq__(self, other):
        return self._appname == other._appname and self._srvtype == other._srvtype

    def __hash__(self):
        return id(self)

    def __init__(self, target):
        self.init(target.url, target.appname, target.srvtype)

    def handle_type(self, prefix, d, metric):
        prefix = prefix.upper()
        if isinstance(d, dict):
            self.extract_dict(prefix, d, metric)
        elif isinstance(d, list):
            self.extract_list(prefix, d, metric)
        else:
            if not isinstance(d, (str, unicode)):
                metric.add_sample('json_val',
                                  value=d, labels={'app_name': self._appname, 'rowkey': prefix})
            else:
                try:
                    val = float(d)
                    metric.add_sample('json_val',
                                      value=val, labels={'app_name': self._appname, 'rowkey': prefix})
                except ValueError:
                    pass
                    # print('Error convert %s to float for %s' % (d, prefix))

    def extract_dict(self, prefix, dd, metric):
        for k, v in dd.items():
            if len(prefix) == 0:
                self.handle_type(k, v, metric)
            else:
                self.handle_type(prefix + self._separator + k, v, metric)

    def extract_list(self, prefix, dd, metric):
        for i in dd:
            if len(prefix) == 0:
                self.handle_type(dd.index(i), i, metric)
            else:
                self.handle_type(prefix + self._separator + dd.index(i), i, metric)

    def collect(self):
        metric = Metric(self._srvtype, 'spark program id', 'summary')
        try:
            resp = requests.get(self._endpoint)
            while len(resp.history) > 0:
                for (new_app_name, new_url) in YarnUtils.get_YARN_apps(self._appname):
                    self._endpoint = new_url

                resp = requests.get(self._endpoint)

            response = json.loads(resp.content.decode('UTF-8'))
            self.handle_type('', response, metric)
            metric.add_sample('spark_up',
                              value=1, labels={'app_name': self._appname})

        # output = []
        #            output.append('# HELP {0} {1}'.format(
        #                metric.name, metric.documentation.replace('\\', r'\\').replace('\n', r'\n')))
        #            output.append('\n# TYPE {0} {1}\n'.format(metric.name, metric.type))
        #            for name, labels, value in metric.samples:
        #                if labels:
        #                    labelstr = '{{{0}}}'.format(','.join(
        #                        ['{0}="{1}"'.format(
        #                         k, v.replace('\\', r'\\').replace('\n', r'\n').replace('"', r'\"'))
        #                         for k, v in sorted(labels.items())]))
        #                else:
        #                    labelstr = ''
        #                output.append('{0}{1} {2}\n'.format(name, labelstr, pcore._floatToGoString(value)))
        #            print('\n'.join(output))
        except Exception, e:
            print('error happens for %s , %s ' % (self._appname, e.message))
            metric.add_sample('scrape_error',
                              value=1, labels={'app_name': self._appname})
        yield metric


class Target():
    def __init__(self, srvtype, appname, url):
        self.srvtype = srvtype
        self.appname = appname
        self.url = url


if __name__ == '__main__':
    # Usage: json_exporter.py port endpoint
    # tmp_1 = JsonCollector(Target('spark_streaming', 'xx', 'yy'))
    # running_cache['xx'] = tmp_1
    # tmps = set()
    # if 'xx' not in running_cache:
    #     tt = JsonCollector(Target('spark_streaming', 'xx', 'yy'))
    #     running_cache['xx'] = tt
    #     tmps.add('xx')
    # else:
    #     print('IN here')
    #
    # print(tmps)
    #
    # for app_name in running_cache.keys():
    #     if not any(app_name1 == app_name for app_name1 in tmps):
    #         print('Going to remove %s registry' % app_name)
    print(sys.argv)
    start_http_server(int(sys.argv[1]))
    # collectors = list()
    # for (app_name, url) in YarnUtils.get_YARN_apps(settings.APP_PATTERN):
    #     tmp_collector = JsonCollector(Target('spark_streaming', app_name, url))
    #     collectors.append(tmp_collector)
    #     running_cache['app_name'] = tmp_collector
    # # targets.append(Target('flume', 'haijun_flume_test1', 'http://10.2.19.94:34545/metrics'))
    # # targets.append(Target('spark_streaming', 'h5_streaming_test1',
    # #                       'http://datanode02.yinker.com:8088/proxy/application_1503367795164_17593/metrics/json'))
    # for col in collectors:
    #     REGISTRY.register(col)
    while True:
        print('start at %s ' % time.strftime("%Y%m%d%H%M%S"))
        tmps = set()
        # for (app_name, url) in YarnUtils.get_YARN_apps(settings.APP_PATTERN):
        print('go to get apps from db')
        app_names = DBUtils.get_spark_apps()

        print('go to get apps from yarn')
        for (app_name, url) in YarnUtils.get_target_apps(app_names=app_names):
            tmps.add(app_name)
            # matchObj = re.match(settings.APP_PATTERN, app_name)
            # if matchObj:
            #     postfix = matchObj.group(1)
            # tmp_collector = JsonCollector(Target('spark_streaming_' + postfix, app_name, url))
            tmp_collector = JsonCollector(Target('spark_streaming_' + app_name, app_name, url))
            if app_name not in running_cache:
                running_cache[app_name] = tmp_collector
                print('Going to add %s collector' % app_name)
                REGISTRY.register(tmp_collector)
                print('added collector for %s' % app_name)

        for app_name in running_cache.keys():
            if not any(app_name1 == app_name for app_name1 in tmps):
                print('Going to remove %s collector' % app_name)
                REGISTRY.unregister(running_cache[app_name])
                running_cache.pop(app_name, 'x')
                print('removed collector for %s' % app_name)
        print 'end ----------------------------\n'
        time.sleep(30)
