#!/usr/bin/env bash

echo "Usage: $0 [option] ...
      -u or --update  : update the version and commit to Github
      -p or --publish : publish the package to PyPI
      -h or --help    : help
"

for arg in "$@"

do
  if [ "$arg" == "--update" ] || [ "$arg" == "-u" ]
  then
      # bump the version
      echo "Updating the package version..."
      OLD_VER=$(cat VERSION)
      NEW_VER=$(echo "$OLD_VER" + .01 | bc)
      echo "$NEW_VER" > VERSION
      echo "Version bumped to $NEW_VER"

      # push to git
      echo "Pushing to GitHub..."
      git commit -m "Bump the version to $NEW_VER"
      git add VERSION
      git commit -a -m "Bump the version to $NEW_VER"
      git push

      # adding tag
      echo "Adding Tag..."
      TAG="v$NEW_VER"
      echo "$TAG"
      git tag -a "$TAG" -m "Bumped the version to $NEW_VER"
      git push origin "$TAG"

  fi

  # publish to PyPI
  if [ "$arg" == "--publish" ] || [ "$arg" == "-p" ]
  then
      echo "Pushing the package to PyPI...
      Note: Please have setuptools, wheel and twine packages installed."

      # creating the distribution package
      rm -rf dist/
      python setup.py sdist
      python setup.py bdist_wheel
      twine upload dist/*

  fi


  # help
  if [ "$arg" == "--help" ] || [ "$arg" == "-h" ]
  then
      echo "Usage: $0 [option] ...
        -u or --update  : update the version and commit to Github
        -p or --publish : publish the package to PyPI
        -h or --help    : help"
  fi
done

