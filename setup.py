import os
from setuptools import setup

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.md')).read()

setup(
    name='django-eb-sqs',
    version='0.1',
    packages=['eb_sqs'],
    description='A SQS worker implementation for Elastic Beanstalk',
    # long_description=README,
    install_requires=[
        'Django>=1.7',
        'boto3>=1.2.3',
    ]
)
