#!/usr/bin/env bash

echo "Installing brew packages..."
brew install openssl graphviz
brew reinstall openssl
echo "Updating matplotlib configuration"
mkdir -p ~/.matplotlib && touch ~/.matplotlib/matplotlibrc
echo "backend: TkAgg" >> ~/.matplotlib/matplotlibrc

