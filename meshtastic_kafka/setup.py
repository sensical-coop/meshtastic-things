# Always prefer setuptools over distutils
from setuptools import setup, find_packages

# To use a consistent encoding
from codecs import open
from os import path

import sys

if sys.version_info < (3,9):
    sys.exit("scdata requires python 3.9")

REQUIREMENTS = [i.strip() for i in open("requirements.txt").readlines()]

setup(
    name='meshtastic-kafka',
    version='0.1.0',
    description='Ingest meshtastic data into kafka queues and influxDB',
    author='oscgonfer',
    license='GNU-GPL3.0',
    packages=find_packages(),
    keywords=['meshtastic', 'kafka', 'influxdb', 'flink'],
    # project_urls=PROJECT_URLS,
    # long_description = open('README.md').read(),
    # long_description_content_type='text/markdown',
    install_requires=[REQUIREMENTS],
    setup_requires=['wheel'],
    python_requires=">=3.9",
    # include_package_data=True,
    # zip_safe=False
    entry_points={
        'console_scripts': [
            'mqtt_kafka_bridge=bridge.mqtt_kafka_bridge:main',
            'kafka_influx_writer=writer.kafka_influx_writer:main'
        ],
    },
)
