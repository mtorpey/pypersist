#!/bin/bash

# This file is used by Travis to run the automated test suite using Sage.  It
# should be run inside a docker container.

echo "start of sage_test.sh"

echo "installing MongoDB..."
sudo apt-get update
sudo apt-get install -y mongodb
echo "done installing!"

echo "starting MongoDB..."
sudo service mongodb start

echo "making pypersist-copy directory..."
cp -r pypersist pypersist-copy

echo "navigating into pypersist-copy directory..."
cd pypersist-copy

echo "installing dependencies..."
sage --pip install pytest-cov codecov eve
echo "done installing!"

echo "running tests..."
sage -python -m pytest --cov=./pypersist
exit $?
