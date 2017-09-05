#! /bin/bash
if [ ! -d "/JsonExporter" ]; then
    git clone https://github.com/ruoyuchen/JsonExporter.git
fi
cd JsonExporter
git pull
python json_exporter.py 1234