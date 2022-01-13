# librarie(s) for accessing system functions (file system and shell commands)
import os
import sys
from re import findall

# library for handling .csv files
import csv

# library for handling REST requests to history server
import requests

# list of result tuples
result_list = []
# input path (second argument including python file itself as first argument)
path = str(sys.argv[1])
# history server address (third argument including python file itself as first argument)
history_server = str(sys.argv[2])

# recursively for all subdirectories find files
for subdir, dirs, filenames in os.walk(path):
    # for the each filename in the files
    for filename in filenames:
        # only check .out files
        if filename.endswith(".out"):
            # construct full path from given parameters
            filename_path = subdir + os.sep + filename

            # split filename into sections
            filename_split = filename.split("-")

            # extract query metadata (dimensions, ...) from filename
            filename_split[2] = int(filename_split[2].split(".")[0][:-1])
            filename_split[3] = int(filename_split[3].split(".")[0][:-1])
            filename_split[4] = int(filename_split[4].split(".")[0][:-1])

            # open corresponding file
            with  open(filename_path, 'r') as filename:
                # read data from file
                data = filename.read()

                # look for timing string and extract time in seconds from it
                times_string = findall(r"Time taken: (\d*\.?\d+) seconds", data)
                # times = [float(item) for item in times_string]
                times = []

                # find application id from file
                app_id = findall(r"Application Id: (.*)", data)
                
                if len(app_id) != 1:
                    pass
                else:
                    # print current application id to indicate progress and for potential debugging
                    print (f"Application Id: {app_id}")

                    # set all metrics to 0
                    total_memory = 0
                    executor_run_time = 0
                    exeturor_cpu_time = 0

                    # request application data
                    response = requests.get(f"{history_server}/api/v1/applications/{app_id[0]}")
                    if response.status_code == 200:
                        # get response as JSON
                        response_json = response.json()
                        # search attempts
                        if 'attempts' in response_json:
                            for attempt in response_json['attempts']:
                                # only consider completed attempts
                                if 'duration' in attempt and 'completed' in attempt and attempt['completed'] == True:
                                    # extract duration from attempt
                                    times = [float(attempt['duration']) / 1000]
                                else:
                                    times = [-1]

                    # get data from history server
                    response = requests.get(f"{history_server}/api/v1/applications/{app_id[0]}/executors")

                    # check if the request to the history server was successful
                    if response.status_code == 200:
                        # get response as JSON
                        response_json = response.json()
                        # get the "executor" metrics from JSON
                        for executor in response_json:
                            # extract memory metrics from executor metrics
                            if 'peakMemoryMetrics' in executor:
                                peak_memory_metrics = executor['peakMemoryMetrics']
                                if 'JVMHeapMemory' in peak_memory_metrics:
                                    total_memory += peak_memory_metrics['JVMHeapMemory']
                                if 'JVMOffHeapMemory' in peak_memory_metrics:
                                    total_memory += peak_memory_metrics['JVMOffHeapMemory']
                            
                            # extract additional timing metrics from executor metrics
                            if 'executorRunTime' in executor:
                                executor_run_time += executor['executorRunTime']
                            if 'executorCpuTime' in executor:
                                exeturor_cpu_time += executor['executorCpuTime']

                    # create a tuple of the result data and append it to the list of result tuples
                    result_list.append((filename_split[0], filename_split[1], "skyline", filename_split[4], filename_split[3], filename_split[2], times[0], total_memory))

# write result to _output.csv file in the same folder as the "most top-level" input folder
with open(path + "/_output.csv", "w") as f:
    writer = csv.writer(f)
    writer.writerows([["algorithm", "dataset", "query", "nodes", "dimensions", "size", "time", "memory"]])
    writer.writerows(result_list)
