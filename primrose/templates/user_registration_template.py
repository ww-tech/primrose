#########################################################################################
#
#
#
#
# *** IMPORTANT ***
#
# We highly recommend setting the environment variable PRIMROSE_EXT_NODE_PACKAGE to define
# the location of your custom node classes. Example: `export PRIMROSE_EXT_NODE_PACKAGE=src/mypackage`
#
# When this variable is set, primrose will automatically register any class
# present in the PRIMROSE_EXT_NODE_PACKAGE path which implements `base.node.AbstractNode`.
#
#
#
#
# Alternatively, if you would like to register classes yourself via `node_factory.NodeFactory`, you can follow the steps below.
#
# Overview
# ========
# There are three steps to registering your own classes:
#
# 1) you need to generate a script to run primrose from your own project. To do that
# run 
# 
#   primrose generate_script --destination path/to/myproject/
# 
# and it copies a run_primrose.py script to your project path
#
# 2) You need to register your own classes. This is the code in this script.
#
# 3) You need to reference this code in the run_primrose script.
#
# Step 1:
# ========
# run
#
#    primrose generate_script --destination path/to/myproject/
#
# which will create path/to/myproject/run_primrose.py
#
# Open up the file and see where it tells you to reference this code.
#
# Step2:
# ========
# Modify this file to register your own classes.
# As can be seen below, 
#  - you first need to import the NodeFactory singleton
#  - next, import your new classes
#  - finally, use NodeFactory to register those classes
#
# Step 3:
# ========
# *Importantly*, this factory registration has to occur *before* Configuration is instantiated
# in the run_primrose script. 
# To that end, we suggest putting this registration code below into something 
# like `src/__init__.py` in your project. Wherever you put it, you will need to reference 
# it in the run_primrose script.
#
# That is, if you put this code into `src/__init__.py`, you will need to add
#
#  from src.__init__ import *
#
# at the head of the run_primrose script. 
#
#########################################################################################

import logging
logging.basicConfig(format='%(asctime)s %(levelname)s %(filename)s %(funcName)s: %(message)s', level=logging.INFO)

from primrose.node_factory import NodeFactory

# Add your imports here
from src.yourpackage.awesome_reader import AwesomeReader
from src.yourpackage.awesome_model import AwesomeModel

NodeFactory().register_module_classes(__name__)
