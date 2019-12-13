import click
import logging
import os
import sys
import importlib
import pkg_resources

logging.basicConfig(format='%(asctime)s %(levelname)s %(filename)s %(funcName)s: %(message)s', level=logging.INFO)


def replace_line(file_name, line_num, text):
    """Replace a single line of a file with text

    Args:
        file_name (str): name of file
        line_num (int): line to replace
        text (str): string to replace at line_num

    Returns:
        Nothing, the file is modified and rewritten

    """
    lines = open(file_name, 'r').readlines()
    lines[line_num] = text
    out = open(file_name, 'w')
    out.writelines(lines)
    out.close()


@click.group()
def cli():
    """This is the command line interface for primrose.
    The command most people need is `primrose run` to run a primrose job.
    Type primrose commandname --help for more detailed help on a command"""
    pass


@click.command()
@click.option('--config', required=True, help="Path to Config file")
def validate(config):
    """Validate a primrose config"""
    from primrose.configuration.configuration import Configuration
    configuration = Configuration(config_location=config)


@click.command()
@click.option('--config', required=True, help="Path to Config file")
@click.option('--dry_run', default=False, help="Config file")
def run(config, dry_run=False):
    """Run a primrose job"""
    from primrose.configuration.configuration import Configuration
    from primrose.dag_runner import DagRunner

    configuration = Configuration(config_location=config)
    DagRunner(configuration).run(dry_run=dry_run)


@click.command()
@click.option('--config', required=True, help="Path to Config file")
@click.option('--node_size', help='Size of nodes', required=False, default=500)
@click.option('--label_font_size', help='Size of font for text labels', required=False, default=12)
@click.option('--text_angle', help='Angle to rotate text (counterclockwise)', required=False, default=0)
@click.option('--image_width', help='width of image in inches', required=False, default=16)
@click.option('--image_height', help='height of image in inches', required=False, default=12)
@click.option('--nodesequence', help='Show node sequences numbers? "true" or "false"?', required=False)
@click.option('--outfile', help='Path for the output image file', required=True)
def plot(config, node_size, label_font_size, text_angle, image_width, image_height, nodesequence, outfile):
    '''Create an image of the DAG'''
    from primrose.configuration.configuration import Configuration
    from primrose.dag.traverser_factory import TraverserFactory

    config = Configuration(config_location=config)

    filename = outfile
    other_args = {}
    other_args['filename'] = outfile

    if node_size:
        other_args['node_size'] = int(node_size)

    if label_font_size:
        other_args['label_font_size'] = int(label_font_size)

    if text_angle:
        other_args['text_angle'] = int(text_angle)

    if image_width:
        other_args['image_width'] = int(image_width)

    if image_height:
        other_args['image_height'] = int(image_height)

    other_args['traverser'] = None
    if nodesequence and nodesequence.lower() == "true":
        traverser = TraverserFactory().default_traverser(config)

        if config.config_metadata and 'traverser' in config.config_metadata:
            traverser = TraverserFactory().instantiate(config.config_metadata['traverser'], config)
            logging.info("Setting to Traverser to %s", config.config_metadata['traverser'])

        other_args['traverser'] = traverser

    logging.info("passing in %s", other_args)
    config.dag.plot_dag(**other_args)


@click.command()
@click.option('--destination', required=True, help="Path to destination")
def generate_run_script(destination):
    """Create primrose run script in your project--if you need to register your own primrose classes.
    This will also allow you to register and use your own primrose node classes. 
    To register your own classes you will also need to run the primrose generate_class_registration_template command"""
    from shutil import copyfile
    run_primrose_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'templates', 'run_primrose.py')
    copyfile(run_primrose_path, destination)
    print("Primrose's run_primrose.py script copied to " + destination)
    print("Don't forget to set where your new classes are to be imported. See script.")


@click.command()
@click.option('--destination', required=True, help="Path to destination")
def generate_class_registration_template(destination):
    """Create template to register your own classes. This will copy a template to your destination, such as src/__init__.py"""
    from shutil import copyfile
    user_registration_template_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                                   'templates',
                                                   'user_registration_template.py')
    copyfile(user_registration_template_path, destination)
    print("Primrose's user_registration_template.py script copied to " + destination)

@click.command()
@click.option('--name', required=True, help="Name of primrose project to be generated")
def create_project(name):
    """Create template project filled with example configuration"""

    from shutil import copyfile, copytree

    # get local directories to copy from the package dir
    package_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'templates')
    env_dir = sys.prefix

    config_dir_path = os.path.join(env_dir, 'config')

    data_dir_path = os.path.join(env_dir, 'data')

    user_registration_template_path = os.path.join(package_dir, 'user_registration_template.py')

    run_primrose_path = os.path.join(package_dir, 'run_primrose.py')

    # make empty cache and src dir
    os.makedirs(os.path.join(name, 'cache'))
    os.makedirs(os.path.join(name, 'src', 'yourpackage'))

    # copy relevant files to user path
    copytree(config_dir_path, os.path.join(name, 'config'))

    copytree(data_dir_path, os.path.join(name, 'data'))

    copyfile(os.path.join(package_dir, 'awesome_model.py'), os.path.join(name,
                                                                         'src',
                                                                         'yourpackage',
                                                                         'awesome_model.py'))

    copyfile(os.path.join(package_dir, 'awesome_reader.py'), os.path.join(name,
                                                                          'src',
                                                                          'yourpackage',
                                                                          'awesome_reader.py'))

    copyfile(user_registration_template_path, os.path.join(name, 'src', '__init__.py'))

    copyfile(run_primrose_path, os.path.join(name, 'run_primrose.py'))

    # modify run_primrose to take into account the templated extention functions
    replacement_line = 'from src.__init__ import *\n'

    replace_line(os.path.join(name, 'run_primrose.py'), 14, replacement_line)

    # make __init__ file for the yourpackage directory
    with open(os.path.join(name, 'src', 'yourpackage','__init__.py'), 'w') as f:
        f.write('')

    print("New primrose project, {} built!".format(name))

@cli.command()
def version():
    '''Print the installed primrose version'''
    print(pkg_resources.get_distribution("primrose").version)

cli.add_command(validate)
cli.add_command(run)
cli.add_command(plot)
cli.add_command(generate_run_script, name='generate-run-script')
cli.add_command(generate_class_registration_template, name='generate-class-registration-template')
cli.add_command(create_project, name='create-project')
cli.add_command(version)

if __name__ == "__main__":
    cli()
