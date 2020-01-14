import os
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
VERSION = open(os.path.join(here, 'VERSION')).read()
README = open(os.path.join(here, 'README.md')).read()

setup(
    name='django-eb-sqs',
    version=VERSION,
    package_dir={'eb_sqs': 'eb_sqs'},
    include_package_data=True,
    packages=find_packages(),
    description='A simple task manager for AWS SQS',
    long_description=README,
    long_description_content_type="text/markdown",
    url='https://github.com/cuda-networks/django-eb-sqs',
    install_requires=[
        'boto3>=1.9.86',
        'Django>=1.10.6',
        'requests>=2.10.0',
    ],
    classifiers=[
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 2.7',
        'Topic :: Software Development',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Framework :: Django'
    ]
)
