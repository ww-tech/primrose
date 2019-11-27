#!/usr/bin/env bash

push_commit() {
  git remote rm origin
  # Add new "origin" with access token in the git URL for authentication
  git remote add origin https://$GITHUB_PERSONAL_ACCESS_TOKEN@github.com:ww-tech/primrose.git > /dev/null 2>&1
  git push origin master --tags --quiet
}

push_commit
