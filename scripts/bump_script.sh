#!/usr/bin/env bash

push_commit() {
  git remote rm origin
  # Add new "origin" with access token in the git URL for authentication
  git remote add origin https://$GITHUB_PERSONAL_ACCESS_TOKEN@github.com/ww-tech/primrose.git > /dev/null 2>&1
  git push origin $TRAVIS_BRANCH --quiet && git push origin $TRAVIS_BRANCH --tags --quiet
}

# use git tag to trigger a build and decide how to increment
current_version=`cat $TRAVIS_BUILD_DIR/.bumpversion.cfg | grep "current_version =" | sed -E s,"^.* = ",,`

if [[ $TRAVIS_EVENT_TYPE != 'pull_request' ]]; then
    if [[ $TRAVIS_BRANCH == *'release'* ]]; then

        if [[ ! $current_version =~ ^(.+dev|.+prod)$ || $BUMP_PART != 'release' ]]; then
            # assume the travis tag is major, minor, or patch to indicate how to increment
            echo "detected current version needs an additional bump before release"
            bump2version $BUMP_PART
        fi
        
        echo "bumping release version"

        message="[skip travis] Bump version: $current_version -> {new_version}"
        bump2version --allow-dirty --tag --commit --message="$message" release
        push_commit

    elif [[ $TRAVIS_BRANCH == 'master' ]]; then
        if ! [[ $current_version =~ ^(.+dev|.+prod)$ ]]; then
            # assume we increment by a patch for dev
            echo "not tagging this release - bump and create dev version"
            bump2version --commit patch
            push_commit
        fi
    fi
else
    echo "on pull request - not bumping"
fi
