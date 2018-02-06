import os
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.md')).read()

setup(
    name='django-eb-sqs',
    version='1.01',
    package_dir={'eb_sqs': 'eb_sqs'},
    include_package_data=True,
    packages=find_packages(),
    description='A SQS worker implementation for Elastic Beanstalk',
    long_description=README,
    url='https://github.com/cuda-networks/django-eb-sqs',
    install_requires=[
        'boto3>=1.4.8',
        'Django>=1.7',
        'redis>=2.10',
        'requests>=2',
    ]
)
