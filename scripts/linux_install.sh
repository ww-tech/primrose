#!/usr/bin/env bash

echo "Installing linux packages"
sudo apt-get install python-dev libgraphviz-dev pkg-config graphviz
echo "Updating matplotlib configuration"
mkdir -p ~/.config/matplotlib && touch ~/.config/matplotlib/matplotlibrc
echo "backend: Agg" >> ~/.config/matplotlib/matplotlibrc
