#!/bin/bash

# This file runs all benchmarks.
# All benchmark queries must be stored in *separate* .sql files.
# This means that each .sql file in the ./input/ folder must contain exactly one query.

./run_real_world_queries.sh
./run_synthetic_queries.sh