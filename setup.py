from setuptools import setup, find_packages
import os

with open('README.md') as f:
    long_description = f.read()

with open('requirements.txt') as f:
    required = f.read().splitlines()

local_data_files = [os.path.join('data', f) for f in os.listdir('data')]
local_config_files = [os.path.join('config', f) for f in os.listdir('config')]

setup(name='primrose',
      version='1.0.9',
      description='Primrose: a framework for simple, quick modeling deployments',
      url='https://github.com/ww-tech/primrose',
      author='Carl Anderson',
      author_email='carl.anderson@weightwatchers.com',
      long_description=long_description,
      long_description_content_type='text/markdown',
      license='Apache 2.0',
      zip_safe=False,
      install_requires=required,
      packages=find_packages(),
      data_files=[('data', local_data_files),
                  ('config', local_config_files)],
      include_package_data=True,
      classifiers=[
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Operating System :: OS Independent"
      ],
      project_urls={
        'Documentation': 'https://ww-tech.github.io/primrose/',
        'Source': 'https://github.com/ww-tech/primrose',
      },
      entry_points={"console_scripts": ["primrose = primrose.__init__:cli"]},
      extras_require={
        'postgres': ["psycopg2>=2.8.3", "psycopg2_binary>=2.8.2"],
        'plotting': ["pygraphviz>=1.5"],
        'R': ["rpy2>=2.9.1"],
        'all': ["psycopg2>=2.8.3", "psycopg2_binary>=2.8.2", "pygraphviz>=1.5", "rpy2>=2.9.1"]
      }
    )