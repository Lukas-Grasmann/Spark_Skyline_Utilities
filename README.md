# Spark_Skyline_Utilities

Utilities for installing dependencies, building, testing, and benchmarking Spark with skyline queries as part of the thesis "Integrating Skyline Queries into
Spark SQL".

The implementation can be found in: https://github.com/Lukas-Grasmann/Spark_3.1.2_Skyline.

**Files may be needed  to be modified according to each system.**

## Repository Contents

* `setup.sh`
  * Setup for both a clean Ubuntu installation.
  * Setup tools needed for Spark development.
  * Options for compiling Spark.
* `data`
  * Sample data which can be used for manual testing of skyline queries in Spark.
* `examples`
  * Some simple example for using skyline queries in Spark.
  * Jupyter files are already pre-executed to provide an example.
  * Contents of .sc and .py files can be run by copying parts to ``spark-shell`` or ``pyspark`` respectively.
  * Code from Jupyter files can generally also be run using ``pyspark`` (beware not to recreate existing ``SparkContext``).
* `testing`
  * **Refer to ``README.md`` in subfolder for more details.**
  * Scripts for setting up and running benchmarks of skyline queries in Spark SQL.
  * Scripts used for generating .csv files used as input to skyline queries tests and benchmarks.
  * R script that can be used to visualize the benchmarks.
  * Jupyter notebook(s) used for testing.

## Setup using `setup.sh`

This setup script was optimized for working on a fresh Ubuntu installation for building Spark with skyline queries. You can use the build utility script by running:

- ```./setup.sh <argument>```

where ``<argument>`` can be one of the following:

- ``basic``OR ``install``
  - install dependencies and automatically clone the repository
  - automatically rename the cloned repository such that all other arguments are working

- ``maven``
  - build Spark found in "spark-3.1.2" using maven
- ``sbt`` OR ``build``
  - build Spark found in "spark-3.1.2" using sbt (recommended)
- ``runnable``
  - build a runnable from the source
  - parameters adapted for use on lbd cluster (https://lbd.zserv.tuwien.ac.at)
  - similar to the distributions normally downloaded from official Spark sources

- ``jekyll``
  - install jekyll and build using jekyll
  - includes compilation of documentation

- ``antlr``
  - Install ANTLR for developing the grammar used by Spark SQL

- ``jupyter``
  - install JUPYTER
  - modify .bashrc as needed using the commands in the comments associated with this option

For building a basic runnable, you can, for example, run the following sequence of commands:
1) ``./setup.sh basic``
2) ``./setup.sh sbt``
3) ``./setup.sh runnable``
