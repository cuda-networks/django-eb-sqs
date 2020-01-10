import os
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.md')).read()

setup(
    name='django-eb-sqs',
    version='1.36',
    package_dir={'eb_sqs': 'eb_sqs'},
    include_package_data=True,
    packages=find_packages(),
    description='A simple task manager for AWS SQS',
    long_description=README,
    url='https://github.com/cuda-networks/django-eb-sqs',
    install_requires=[
        'boto3>=1.9.86',
        'Django>=1.10.6',
        'requests>=2.10.0',
    ]
)
