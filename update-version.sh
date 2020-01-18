#!/bin/bash

set -e

VERSION_PARAM=${1:-AUTO}

function log()
{
  echo "$(date): $1"
}

function usage()
{
  echo "Helper to update the package version and commit to GitHub.
  Usage: $0 <VERSION_NUMBER>
  <VERSION_NUMBER> : Version is usually a semantic release eg 1.20, 1.1.7.
                     VERSION value is optional, if not passed then an auto version update occurs.
  -h               : help
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

# Parse and handle command line options
while getopts ":h" OPTION; do
  case $OPTION in
  h)
    usage
    ;;
  *)
    usage
    ;;
  esac
done

update
