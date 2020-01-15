#!/bin/bash

function log()
{
  echo "$(date): $1"
}

function usage()
{
  echo "Usage: $0 [option] ...
  -u <VERSION> : Update the package version and commit to Github. Version is usually a semantic release eg 1.20, 1.1.7
                 If you want to do an auto update use '-u AUTO'.
  -p <TAG>     : Publish the package to PyPI. Specify the Github repo tag we want to use to create the distribution
                 package. eg: '-p v1.36'
  -h           : help
  "
  exit
}

function update()
{
  # update codebase
  git checkout master
  git pull

  if [ "$VERSION_PARAM" = "AUTO" ]
  then
      OLD_VER=$(cat VERSION)
      NEW_VER=$(echo "$OLD_VER" + .01 | bc)
  else
      NEW_VER=$VERSION_PARAM
  fi

  # bump the version
  echo "$NEW_VER" > VERSION
  log "Version bumped to $NEW_VER"

  # push VERSION file and new TAG to git
  log "Pushing to GitHub..."
  git add VERSION
  git commit -a -m "Bump the version to $NEW_VER"
  git push

  # adding tag
  log "Adding Tag..."
  TAG="v$NEW_VER"
  log "New tag : $TAG"
  git tag -a "$TAG" -m "Bumped the version to $NEW_VER"
  git push origin "$TAG"

  exit
}

function publish()
{
  log "Pushing the package to PyPI. (Note: Please have setuptools, wheel and twine packages installed.)"
  log "Publishing package version/tag: $TAG_PARAM"

  # checkout specific tag version
  git checkout tags/"$TAG_PARAM"

  # creating the distribution package
  rm -rf dist/
  python setup.py sdist
  python setup.py bdist_wheel
  twine upload dist/*

  exit
}

# Parse and handle command line options
while getopts ":u:p:" OPTION; do
	case $OPTION in
	u)
		VERSION_PARAM=$OPTARG
		update
		;;
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