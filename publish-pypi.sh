#!/bin/bash

set -e

function log()
{
  echo "$(date): $1"
}

function usage()
{
  echo "Helper to publish the package to PyPI.
  Usage: $0 -p <GITHUB_TAG>
  -p <GITHUB_TAG> : Github Tag value, semantic realease format eg: v1.36 or v2.0.7
  -h              : help
  "
  exit
}

function publish()
{

  log "Installing required dependencies"
  pip install -r requirements-pypi.txt

  # checkout specific tag version
  git checkout tags/"$TAG_PARAM"

  # creating the distribution package
  rm -rf dist/
  python setup.py sdist
  python setup.py bdist_wheel

  log "Publishing package version/tag: $TAG_PARAM"
  twine upload dist/*  #  add --repository-url https://test.pypi.org/legacy/ to push to TestPyPI

  exit
}

# Parse and handle command line options
while getopts ":p:h" OPTION; do
  case $OPTION in
  p)
    TAG_PARAM=$OPTARG
    publish
    ;;
  *)
    usage
    ;;
  esac
done

# if no args specified
if [ "$#" -ne 1 ]
then
  usage
fi
