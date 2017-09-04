from prometheus_client import core as pcore, start_http_server, Metric, REGISTRY
import json
import requests
import sys
import time

from utils import YarnUtils


class JsonCollector():
    def init(self, endpoint, appname, srvtype, separator='__'):
        self._endpoint = endpoint
        self._appname = appname
        self._separator = separator
        self._srvtype = srvtype

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
                    print('Error convert %s to float for %s' % (d, prefix))

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
        try:
            response = json.loads(requests.get(self._endpoint).content.decode('UTF-8'))
            metric = Metric(self._srvtype, 'spark program id', 'summary')
            self.handle_type('', response, metric)
            yield metric

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
            print e.message


class Target():
    def __init__(self, srvtype, appname, url):
        self.srvtype = srvtype
        self.appname = appname
        self.url = url


if __name__ == '__main__':
    # Usage: json_exporter.py port endpoint

    print(sys.argv)
    start_http_server(int(sys.argv[1]))
    targets = list()
    for (app_name, url) in YarnUtils.get_YARN_apps('*h5.*'):
        targets.append(Target('spark_streaming', app_name, url))
    # targets.append(Target('flume', 'haijun_flume_test1', 'http://10.2.19.94:34545/metrics'))
    # targets.append(Target('spark_streaming', 'h5_streaming_test1',
    #                       'http://datanode02.yinker.com:8088/proxy/application_1503367795164_17593/metrics/json'))
    for target in targets:
        REGISTRY.register(JsonCollector(target))
    # REGISTRY.register(JsonCollector())

    while True: time.sleep(10)
