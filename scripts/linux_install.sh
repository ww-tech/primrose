#!/usr/bin/env bash

echo "Installing linux packages"
sudo apt-get update
sudo apt-get install libgraphviz-dev pkg-config graphviz
dpkg -L libgraphviz-dev
echo "Updating matplotlib configuration"
mkdir -p ~/.config/matplotlib && touch ~/.config/matplotlib/matplotlibrc
echo "backend: Agg" >> ~/.config/matplotlib/matplotlibrc
