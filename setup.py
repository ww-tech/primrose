from setuptools import setup, find_packages

with open('README.md') as f:
    long_description = f.read()

with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(name='primrose',
      version='1.0.0',
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
      classifiers=[
        "Programming Language :: Python :: 3.6",
        "Operating System :: OS Independent"
      ],
      project_urls={
        'Documentation': 'https://ww-tech.github.io/primrose/',
        'Source': 'https://github.com/ww-tech/primrose',
      },
      entry_points={"console_scripts": ["primrose = primrose.__init__:cli"]},
    ),
