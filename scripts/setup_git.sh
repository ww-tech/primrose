#!/usr/bin/env bash

setup_git() {
  git config --global user.email "travis@travis-ci.org"
  git config --global user.name "Travis CI"
  # Remove existing "origin"
  git remote rm origin
  # Add new "origin" with access token in the git URL for authentication
  git remote add origin https://$GITHUB_PERSONAL_ACCESS_TOKEN@github.com:ww-tech/primrose.git > /dev/null 2>&1
}

setup_git
