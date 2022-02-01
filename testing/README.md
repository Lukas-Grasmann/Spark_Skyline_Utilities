# Skylines in Spark - Testing and Benchmarking

This folder contains the utilities and documents used to test and benchmark the implementation of skyline queries in Spark.

## Data Generation

In this section, we discuss how the data for the tests can be generated from the input. For this purpose, we use the `data_generation` directory. The subfolder `processing` contains an `R` script which automatically computes the data based on the contents of the `input` folder and puts the results in the `output` folder.

For the `R` script to work, the libraries `dplyr` and `readr` are needed.

All output files ending with `test` belong to the tests while all other output files belong to the benchmarks.

The input must be placed in the `input/<dataset>` folders with the following naming conventions:
* Airbnb: Place the .csv files for each region/city in a `cities` subfolder in the input directory. The naming is irrelevant in this case
* Fueleconomy: use `vehicles.csv`
* COIL 2000: use `ticdata2000.txt`
* NBA: use `stats.csv`
* Store Sales (DSB): use `store_sales.csv`

For the Airbnb dataset, note when copying the processed data for the Inside Airbnb dataset, note that only the files `airbnb.csv`, `airbnb_incomplete.csv`, and `airbnb_test.csv`, `airbnb_incomplete_test.csv` are currently used for benchmarking and testing respectively. All other files from the `cities` subfolder only contain partial results for each respective city/region which are not used in any test or benchmark currently.

**NOTE: This repository does not contain the input .csv files to reduce size (down from several GB). Please add them manually from their respective sources.**

Links to the datasets can be found in Planning.docx

## Testing

In this section, we discuss setting up the all-up testing and running the tests.

### Setup

Very little setup is necessary to run the test cases since they are governed by the Jupyter notebook `tests.ipynb` and rely on `.csv` files from the `input` folder as datasource.

Provided that all necessary dependencies are installed (Java, Spark, Python, Jupyter), the only modification needed can be done by setting environment variables via `.bashrc`:

```
export SPARK_HOME=/spark-home/
export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.10.9-src.zip:$PYTHONPATH
export PYSPARK_DRIVER_PYTHON="jupyter"
export PYSPARK_DRIVER_PYTHON_OPTS="notebook"
export PYSPARK_PYTHON=python3
export PATH=$SPARK_HOME:$PATH
```

For Hive configuration, we can add the following lines:

```
export HIVE_HOME/hive-home/
export PATH=$HIVE_HOME/bin:$PATH
```

### Running the Tests

Running the tests can be done by simply using `jupyter notebook` and then using the web interface to run the tests. (Tip: Use Cells -> Run All to run all tests at once.)

If execution without a graphical user interface from the terminal is desired, then `nbconvert` can be used for that purpose:

```
jupyter nbconvert --execute tests.ipynb
```

This will first convert the `.ipynb` file to a compiled notebook and subsequently execute it. The results can then be found in the notebook.

## Benchmarking

In this section, we discuss setting up the benchmarks and running them.

***NOTE: The database `benchmarks` serves as example and default in all scripts. It can be changed by modifying the `database` variable in all scripts.***

### Setup

First, the database `benchmarks` must be created manually. For example, for a standalone cluster on a *nix system, this can be done using the following commands:

```
./spark-sql \
    --master spark://<address>:<port> \
    --conf spark.sql.catalogImplementation=hive
```

Followed by:

```
CREATE DATABASE benchmarks;
```

Subsequently, the tables can be created using the `setup.py` script. It can be fed directly into `spark-submit` and will create the corresponding tables. To create only the real-world datasets, use `setup_real_world.py` instead.

**Additional setup may be needed depending on which datasources are used.**

### Faster Setup Using HIVE

When using HIVE as datasource, the files `hive_setup_real_world.sql` and `hive_setup_synthetic.sql` can be used instead of the setup using Spark.

The files contain HiveQL queries which can also set up the database directly.

Replace `input_home` in the .sql files to set the path of the input .csv files. Absolute paths may be preferable to prevent errors.

### Running the Benchmarks

The benchmarks can be run by simply calling the corresponding `.sh` scripts. These will create the necessary `.sql` files in the `input` folder and try to run the queries on Spark.

For this to work, the environment variable `SPARK_HOME` must be set correctly. The variable `run_args` must be modified such that it contains the correct configuration for Spark and Spark is run using the correct cluster.

For example, if the setup from above is used then `run_args` may look like this:

```
run_args = "--master spark://<address>:<port> --conf spark.sql.catalogImplementation=hive"
```

The output will of each query will be written in full to a text file in the `output` subfolder.

***NOTE:*** **This repository does not contain the input `.csv` files to reduce size (down from several GB). Please add them manually from their respective sources.**

### Collecting the results

Since the queries themselves only write the full output without any processing to the `output` folder, it is necessary to subsequently collect all times from the output. All other relevant data is known since the script which generates the query also handles running it and preserves the data used for generating the query.

For this purpose, the script `collect_values.py` can be used. It assumes that the results of the query execution lie in `.out` files somewhere in the path which is provided to `collect_values.py` as a parameter. It will process all such files and determine the parameters of the execution by the filenames.

The filename

```
reference-airbnb-100t-1d-1n.sql.out
```

is interpreted as the result of the `reference` algorithm on the `airbnb` dataset of `100` tuples with a `1` dimensional skyline and `1` node.

Note that the meaning of "node" may vary between benchmarks. It may refer to cores or executors instead.

We could now find the execution time by string matching using `r"Time taken: (\d*\.?\d+) seconds"` and generates a row of data. Instead, we use the history server which must e running when using `collect_values.py`.

For this, we extract the application id using `r"Application Id: (.*)"` and use the REST API of the history server as follows.

Extract application metrics (e.g., duration):
```
response = requests.get(f"{history_server}/api/v1/applications/{app_id[0]}")
```

Extract the executor metrics (e.g., memory consumption):
```
response = requests.get(f"{history_server}/api/v1/applications/{app_id[0]}/executors")
```

Once all files are processed, all such rows are written to an `_output.csv` file which is placed in the path which was passed to `collect_values.py`. This `.csv` file can then be used for further analysis and visualization. Note that existing files of the same name will be overwritten.

### Plotting the results

The R-script `benchmarks.R` can be used to plot the data extracted in the previous step. It loads the data from `data/output.csv` which corresponds to a .csv file created by "collecting the results". The path can be replaced in the script.

This script will automatically process the data from said file and output the plots as .png images in the **same** folder the R-script is in. Existing plots will be overwritten.
