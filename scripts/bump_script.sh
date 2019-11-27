#!/usr/bin/env bash

# use git tag to trigger a build and decide how to increment
current_version=`cat $TRAVIS_BUILD_DIR/.bumpversion.cfg | grep "current_version =" | sed -E s,"^.* = ",,`

if [[ $TRAVIS_BRANCH == *'release'* ]]; then

    if [[ ! $current_version =~ ^(.+dev|.+prod)$ || $BUMP_PART != 'release' ]]; then
        # assume the travis tag is major, minor, or patch to indicate how to increment
        echo "detected current version needs an additional bump before release"
        bump2version $BUMP_PART
    fi
    
    echo "bumping release version"

    message="[skip travis] Bump version: $current_version -> {new_version}"
    bump2version --allow-dirty --tag --commit --message=$message release

elif [[ $TRAVIS_BRANCH == 'master' ]]; then
    if ! [[ $current_version =~ ^(.+dev|.+prod)$ ]]; then
        # assume we increment by a patch for dev
        echo "not tagging this release - bump and create dev version"
        bump2version --commit patch
    fi
fi
