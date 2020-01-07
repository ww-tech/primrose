# Overview
[![Build Status](https://travis-ci.org/ww-tech/primrose.svg?branch=master)](https://travis-ci.org/ww-tech/primrose)
[![PyPI pyversions](https://img.shields.io/pypi/pyversions/primrose.svg)](https://pypi.python.org/pypi/primrose/)
[![PyPI version](https://badge.fury.io/py/primrose.svg)](https://badge.fury.io/py/primrose)
[![PyPI license](https://img.shields.io/pypi/l/primrose.svg)](https://pypi.python.org/pypi/primrose/)
[![Docs status](https://img.shields.io/website/https/ww-tech.github.io/primrose?down_color=red&down_message=docs&label=docs&up_color=success&up_message=up)](https://ww-tech.github.io/primrose/)


<p align="center">
   <img src="img/primrose_logo.png" width="100">
</p>

## Primrose at a glance

`Primrose` is a simple **Python** framework for executing **in-memory** workflows defined by directed acyclic graphs (**DAGs**) via configuration files. Data in `primrose` flows from one node to another while **avoiding serialization**, except for when explicitly specified by the user. `Primrose` nodes are designed for **simple batch-based machine learning workflows**, which have datasets small enough to fit into a single machine's memory.

## Table of Contents
We suggest reading the documentation in the following order:

 - Overview and motivation for `primrose`&mdash;this file.
 - [Getting Started](README_GETTING_STARTED.md): run your first `primrose` jobs.
 - [DAG Configurations](README_DAG_CONFIG.md): `primrose` adopts a configuration-as-code paradigm. This section introduces `primrose` configuration files.
 - [Metadata](README_METADATA.md): this covers more advanced options of the configuration files.
 - [Command Line Interface (CLI)](README_CLI.md): run commands using the CLI.
 - [Developer Notes](README_DEVELOPER_NOTES.md): how to create your own new Node classes.
 - [DataObject](README_DATAOBJECT.md): a deep dive into `DataObject`, the core data handling and book-keeping object.

## Introduction

 `Primrose` is a Python framework for quick, simple machine learning and recommender deployments developed by the data science team at [WW](https://www.weightwatchers.com/us/). It is essentially a workflow management tool which is specialized for the needs of machine learning tasks with small to medium sized datasets (&le; 100GB). Like many orchestration tools, `Primrose` *nodes* are defined in a [directed-acyclic-graph](https://en.wikipedia.org/wiki/Directed_acyclic_graph) which defines dependencies and control flow.

 Here's an example DAG showing data cleaning, model training, and model serialization:

 <p align="center">
   <img src="img/hello_world_tennis.png" width="500">
</p>

 It exists within an ecosystem of other great open source workflow management tools (like [Airflow](https://airflow.apache.org/), [Luigi](https://luigi.readthedocs.io/en/stable/), [Kubeflow](https://www.kubeflow.org/docs/about/kubeflow/) or [Prefect](https://docs.prefect.io/guide/)) while carving it's own niche based on the following design goals:

1. **Avoid unnecessary serialization:** `Primrose` keeps data in-memory between task steps, and only performs (de)serialization operations when explicitly requested by the user. Data is transported between nodes through use of a `DataObject` abstraction, which contextually delivers the correct data to each `Primrose` node at runtime. As a consequence of this design choice, `Primrose` runs on a single machine and can be deployed as a job within a single container, like any other Python script or cron job. In addition to operating on persistent data passed between nodes, `Primrose` can also be used to call external services in a manner similar to a [Luigi](https://luigi.readthedocs.io/en/stable/) job. In this way, Spark jobs or Hadoop scripts can be called and the framework simply dictates dependencies.

    * *As a comparison...* many solutions in this space are focused on long-running jobs which may be distributed across several computing nodes. Furthermore, to facilitate parallelization, save states for redundancy, and process datasets which are too large for memory, orchestrators often require that data is serialized between each workflow task. For smaller datasets, the IO time associated with these steps can be much longer than the time spent in computation.

    * *Primrose is not...* a solution which scales across clusters or a complex dependency management solution with dynamic DAGs (yet).

2. **Batch processesing for ML:** `Primrose` was built to facilitate frequent batches of model training or predictions that must read and write from/to multiple sources. Rather than requiring users to define their DAG structure in Python code, `Primrose` adopts a `configuration-as-code` approach. `Primrose` users create implementations of node objects once, then any DAG structural modifications or parameterization changes are processed through configuration json files. This way, deployment changes to DAG operations (such as modifying a DAG to serve model predictions instead of training) can be handled purely through configuration files. This avoids the need to build new Python scripts for production modifications. Furthermore, `Primrose` nodes are based on common machine learning tasks to make data scientist's lives easier. This cuts down on development time for building new models and maximizes code re-use among projects and teams. See the modeling examples in the source and documentation for more info!
    * *As a comparison...* in `Primrose`, users simply need to specify in their configuration file that they want common ML operations to act on the `DataObject`. These ML operations can certainly be implemented by users in Luigi or Airflow, but we found operations such as test-train splits or classifier cross-validation to be so common that they warranted nodes pre-dedicated to these operations.  [Prefect](https://docs.prefect.io/guide/) has made some great strides in this area, and we encourage users to check their solution out.

    * *Primrose is not...* an auto-ml tool or machine-learning toolkit which implements its own algorithms. Any Python machine learning library can be used with `Primrose`, simply by building model or pipeline nodes that implement the user's choice of library.

3. **Simplicity:**

    **Standardization of deployments:** `Primrose` is meant to help make deployment and model building as simple as possible. From a developer operations perspective, it requires no external scheduler or cluster to run deployments. `Primrose` code can simply be containerized with a `primrose` Python entrypoint, and deployed as a job on a k8s or any other container management service.

    **Standardization of development:** From a software engineering perspective, another advantage of `Primrose` stems form the standardization of model and recommender code. Modifying feature engineering pipelines or adding recommender features is simplified by writing additions to self-contained `Primrose` nodes and making additions to a configuration file.

    * *As a comparison...* `Primrose` can be leveraged as a piece of a larger ETL job (a `Primrose` job could be a job within an Airflow DAG), or run on it's own as a self-contained, single node ETL job. Some orchestration solutions (Airflow, for example) require running persistent clusters and services for managing jobs.

    * *Primrose is not...* able to manage its own job scheduling or timing. This is left to user using k8s job scheduling or manual cron job assignments on a virtual machine.


There are many solutions in this space, and we encourage users to explore other options that may be most appropriate for their workflows. We view `Primrose` as a simple solution for managing production ML jobs.


## Getting Started

`Primrose` has a couple of optional tools:
* a postgres database reader
* a plotting tool

These require a few external dependencies, prior to its installation. If interested in their functionality, follow the appropriate instructions for your OS below. Otherwise, you can proceed with the basic package installation.

### Installation

You can install the latest `Primrose` release via pypi
```
pip install primrose
```
or you can clone the repository and install via `setup.py`.
```
git clone https://github.com/ww-tech/primrose.git
cd primrose
python setup.py install
```

To install the complete `Primrose` package (after dependencies have been installed):
```
    pip install primrose[postgres, plotting]
```

To install `Primrose` with just the postgres option:

```
pip install primrose[postgres]
```

To install `primrose` with just the plotting option:

```
pip install primrose[plotting]
```

### External dependenices

**Postgres**

#### MacOSX

We recommend using homebrew to manage OS level external packages. If you do not already have homebrew installed, please visit [their website](https://brew.sh/).

Instructions:
1. Use homebrew to install `postgresql` library.
   ```
   brew install postgresql
   ```

2. Use `pip` to install `psycopg2`
   ```
    pip install psycopg2
   ```

#### Debian/Ubuntu

Instructions:
1. Install the `libpq-dev` library
   ```
   apt-get install libpq-dev
   ```

**Plotting**

#### MacOSX

We recommend using homebrew to manage OS level external packages. If you do not already have homebrew installed, please visit [their website](https://brew.sh/).

Instructions:
1. Use homebrew to install `graphviz` library.
   ```
   brew install graphviz
   ```
2. If you are using a virtual environment such as `Anaconda` or `virtualenv`, you may need to specify a backend for `matplotlib`.
   ```
   mkdir -p ~/.matplotlib && touch ~/.matplotlib/matplotlibrc
   echo backend: TkAgg >> ~/.matplotlib/matplotlibrc
    ```

#### Debian/Ubuntu

Instructions:
1. Install `graphviz` library.
   ```
   apt-get install graphviz
   ```
2. If you are using a virtual environment such as `Anaconda` or `virtualenv`, you may need to specify a backend for `matplotlib`.
   ```
   mkdir -p ~/.config/matplotlib && touch ~/.config/matplotlib/matplotlibrc
   echo backend: Agg >> ~/.config/matplotlib/matplotlibrc
   ```

## Next
You are now ready to run your first `primrose` jobs: [Getting Started](README_GETTING_STARTED.md).

## License
Copyright 2019 WW International, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.