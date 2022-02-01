# Spark_Skyline_Utilities

Utilities for installing dependencies, building, testing, and benchmarking Spark with skyline queries.

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
  * Jupyter file is already pre-executed to provide an example.
  * Contents of .sh files can be run using spark-shell.
* `testing`
  * **Refer to README in subfolder for more details.**
  * Scripts for setting up and running benchmarks of skyline queries in Spark SQL.
  * Scripts used for generating .csv files used as input to skyline queries tests and benchmarks.
  * R script that can be used to visualize the benchmarks.
  * Jupyter notebook(s) used for testing.
