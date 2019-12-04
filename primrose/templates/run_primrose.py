'''
    Run a job: i.e. run a configuration file through the DAGRunner
'''
import argparse
import logging
import warnings

######################################
######################################
# Important:
#
# If your configuration uses custom node classes, be sure to set environment variable
# PRIMROSE_EXT_NODE_PACKAGE to the location of your package before running primrose.
# Example:
#   ```
#   export PRIMROSE_EXT_NODE_PACKAGE=src/mypackage
#   python run_primrose.py --config_loc my_config.json
#   ```
#
######################################
######################################

from primrose.configuration.configuration import Configuration
from primrose.dag_runner import DagRunner
from primrose.dag.config_layer_traverser import ConfigLayerTraverser
from primrose.dag.depth_first_traverser import DepthFirstTraverser

warnings.filterwarnings("ignore")

def parse_arguments():
    """
        Parse command line arguments

        Returns:
            argument objects with flags as attributes
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--config_loc',
                        help='Location of the configuration file',
                        required=True)
    parser.add_argument('--is_dry_run',
                        help='do a dry run of the DAG which will validatre config and log which nodes would be run',
                        default=False,
                        type=lambda x: (str(x).lower() == 'true'))

    known_args, pipeline_args = parser.parse_known_args()
    return known_args, pipeline_args

def main():
    """
        Run a job: i.e. run a configuration file through the DAGRunner
    """
    args, _ = parse_arguments()

    logging.basicConfig(format='%(asctime)s %(levelname)s %(filename)s %(funcName)s: %(message)s', level=logging.INFO)

    configuration = Configuration(config_location=args.config_loc)

    DagRunner(configuration).run(dry_run=args.is_dry_run)

if __name__ == '__main__':
    main()
