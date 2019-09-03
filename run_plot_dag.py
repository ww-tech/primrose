import logging
import argparse

from primrose.configuration.configuration import Configuration
from primrose.dag.traverser_factory import TraverserFactory

def parse_arguments():
    """Parse command line arguments
    Use environment variables as default if passed.
    Returns: argument objects with flags as attributes
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--config_loc',
                        help='Location of the configuration file',
                        required=True)
    parser.add_argument('--node_size',
                        help='Size of nodes',
                        required=False)
    parser.add_argument('--label_font_size',
                        help='Size of font for text labels',
                        required=False)
    parser.add_argument('--text_angle',
                        help='Angle to rotate text (counterclockwise)',
                        required=False)
    parser.add_argument('--image_width',
                        help='width of image in inches',
                        required=False)
    parser.add_argument('--image_height',
                        help='height of image in inches',
                        required=False)
    parser.add_argument('--nodesequence',
                        help='Show node sequences numbers? "true" or "false"?',
                        required=False)
    parser.add_argument('--outfile',
                        help='Path for the output image file',
                        required=True)
    known_args, pipeline_args = parser.parse_known_args()
    return known_args, pipeline_args

def main():
    """Train, evaluate or predict with a machine learning model from a user defined data source
        The function can optionally upload the results to an external source
    """
    args, _ = parse_arguments()

    logging.basicConfig(format='%(asctime)s %(filename)s %(funcName)s: %(message)s', level=logging.INFO)
    logging.getLogger().setLevel(logging.INFO)

    config = Configuration(config_location=args.config_loc)

    filename = args.outfile
    other_args = {}
    other_args['filename'] = args.outfile

    if args.node_size:
        other_args['node_size'] = int(args.node_size)

    if args.label_font_size:
        other_args['label_font_size'] = int(args.label_font_size)

    if args.text_angle:
        other_args['text_angle'] = int(args.text_angle)

    if args.image_width:
        other_args['image_width'] = int(args.image_width)

    if args.image_height:
        other_args['image_height'] = int(args.image_height)

    other_args['traverser'] = None
    if args.nodesequence and args.nodesequence.lower() == "true":
        traverser = TraverserFactory().default_traverser(config)

        if config.config_metadata and 'traverser' in config.config_metadata:
            traverser = TraverserFactory().instantiate(config.config_metadata['traverser'], config)
            logging.info("Setting to Traverser to %s", config.config_metadata['traverser'])

        other_args['traverser'] = traverser

    logging.info("passing in %s", other_args)
    config.dag.plot_dag(**other_args)

if __name__ == '__main__':
    main()
