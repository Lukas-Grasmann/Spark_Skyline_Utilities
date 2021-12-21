#!/bin/bash

# Use this file for generating queries for benchmarking and/or testing.
# Each query will be put into a separate .sql file which may contain either skyline syntax or "plain" SQL.
# Files are named according to their contents.
#
# All "reference" files contain play SQL and are generated for each potential skyline query in this file.
# They represent reference queries which should yield the same results as a correctly implemented skyline algorithm
#
# All other files use one skyline algorithm via specialized skyline syntax to get the results.
#
# WARNING: For each dataset-dimensions combination there is excatly one reference solution.
# WARNING: There may be multiple skyline algorithms for this combination which all correspond to the **same** reference solution.

database="benchmarks"

run_args="--master yarn \
--deploy-mode client \
--conf spark.sql.catalogImplementation=hive \
--conf spark.executor.processTreeMetrics.enabled=true \
--conf spark.executor.metrics.pollingInterval=10 \
--conf spark.executor.heartbeatInterval=1000"

algorithms=("bnl" "dist" "dist_inc")
datasets_complete=("airbnb" "fueleconomy" "coil2000" "nba")
datasets_incomplete=("airbnb_incomplete" "fueleconomy_incomplete"
                     "coil2000_incomplete" "nba_incomplete")

num_dimensions=$(seq 1 6)
num_nodes=$(seq 1 5)

declare -A dimensions
dimensions[airbnb]="price accommodates bedrooms beds number_of_reviews review_scores_rating"
dimensions[fueleconomy]="fuelCost08 barrels08 city08 highway08 comb08 combinedCD"
dimensions[coil2000]="MOSHOOFD MGODRK MGODPR MGODGE MRELGE MRELOV"
dimensions[nba]="W L W_PCT FGM FGA FG_PCT"

dimensions[airbnb_incomplete]="price accommodates bedrooms beds number_of_reviews review_scores_rating"
dimensions[fueleconomy_incomplete]="fuelCost08 barrels08 city08 highway08 comb08 combinedCD"
dimensions[coil2000_incomplete]="MOSHOOFD MGODRK MGODPR MGODGE MRELGE MRELOV"
dimensions[nba_incomplete]="W L W_PCT FGM FGA FG_PCT"

declare -A minmaxdiff
minmaxdiff[airbnb]="MIN MAX MAX MAX MAX MAX"
minmaxdiff[fueleconomy]="MIN MIN MAX MAX MAX MIN"
minmaxdiff[coil2000]="MAX MIN MIN MAX MAX MAX"
minmaxdiff[nba]="MAX MIN MAX MAX MAX MAX"

minmaxdiff[airbnb_incomplete]="MIN MAX MAX MAX MAX MAX"
minmaxdiff[fueleconomy_incomplete]="MIN MIN MAX MAX MAX MIN"
minmaxdiff[coil2000_incomplete]="MAX MIN MIN MAX MAX MAX"
minmaxdiff[nba_incomplete]="MAX MIN MAX MAX MAX MAX"

declare -A tuples
tuples[airbnb]=826838
tuples[fueleconomy]=44071
tuples[coil2000]=5822
tuples[nba]=446

tuples[airbnb_incomplete]=1201556
tuples[fueleconomy_incomplete]=44404
tuples[coil2000_incomplete]=5822
tuples[nba_incomplete]=446

destination_folder="input/queries/real_world"
mkdir -p ${destination_folder}

output_folder="output/real_world"
mkdir -p ${output_folder}

data_folder="input/data"

for nodes in ${num_nodes}
do
    for dataset in ${datasets_complete[@]}
    do
        for dimension in ${num_dimensions[@]}
        do  
            filename=reference-${dataset}-${tuples[${dataset}]}t-${dimension}d-${nodes}n.sql
            creation_path=${destination_folder}/${filename}
            absolute_path=$(realpath ${creation_path})

            dimlist=(${dimensions[${dataset}]})
            minmaxlist=(${minmaxdiff[${dataset}]})

            echo -n "creating ${filename} ... "
            
            touch $creation_path

            sql_query="SELECT \* FROM ${database}.${dataset} AS o WHERE NOT EXISTS(\n"
            sql_query+=" SELECT \* FROM ${database}.${dataset} AS i WHERE\n"

            for i in $(seq 1 ${dimension}); do
                dimname="${dimlist[${i}-1]}"
                minmax="${minmaxlist[${i}-1]}"

                if [[ $minmax == "MIN" ]]; then
                    sql_query+=" i.${dimname} <= o.${dimname} AND\n"
                elif [[ $minmax == "MAX" ]]; then
                    sql_query+=" i.${dimname} >= o.${dimname} AND\n"
                fi
            done

            sql_query+="  (\n"

            for i in $(seq 1 ${dimension}); do
                dimname="${dimlist[${i}-1]}"
                minmax="${minmaxlist[${i}-1]}"

                if [[ $minmax == "MIN" ]]; then
                    sql_query+=" i.${dimname} < o.${dimname}"
                elif [[ $minmax == "MAX" ]]; then
                    sql_query+=" i.${dimname} > o.${dimname}"
                fi

                if [[ $i != ${dimension} ]]; then
                    sql_query+=" OR\n"
                fi
            done

            sql_query+="\n  )\n)\n"

            echo -en $sql_query > $creation_path
            sed -i 's/\\\*/\*/g' $creation_path

            echo "ok"

            echo -n "running query ... "

            output_path=$(realpath ${output_folder}/${filename}.out)

            touch ${output_path}

            /.${SPARK_HOME}/bin/spark-sql \
                ${run_args} \
                --num-executors ${nodes} \
                -f ${absolute_path} \
                &> ${output_path}

            echo "ok"
        done
    done

    for dataset in ${datasets_incomplete[@]}
    do
        for dimension in ${num_dimensions[@]}
        do
            filename=reference-${dataset}-${tuples[${dataset}]}t-${dimension}d-${nodes}n.sql
            creation_path=${destination_folder}/${filename}
            absolute_path=$(realpath ${creation_path})

            dimlist=(${dimensions[${dataset}]})
            minmaxlist=(${minmaxdiff[${dataset}]})

            echo -n "creating ${filename} ... "
            
            touch $creation_path

            sql_query="SELECT \* FROM ${database}.${dataset} AS o WHERE NOT EXISTS(\n"
            sql_query+=" SELECT \* FROM ${database}.${dataset} AS i WHERE\n"

            for i in $(seq 1 ${dimension}); do
                dimname="${dimlist[${i}-1]}"
                minmax="${minmaxlist[${i}-1]}"

                if [[ $minmax == "MIN" ]]; then
                    sql_query+=" (i.${dimname} <= o.${dimname} OR i.${dimname} IS NULL OR o.${dimname} IS NULL) AND\n"
                elif [[ $minmax == "MAX" ]]; then
                    sql_query+=" (i.${dimname} >= o.${dimname} OR i.${dimname} IS NULL OR o.${dimname} IS NULL) AND\n"
                fi
            done

            sql_query+="  (\n"

            for i in $(seq 1 ${dimension}); do
                dimname="${dimlist[${i}-1]}"
                minmax="${minmaxlist[${i}-1]}"

                if [[ $minmax == "MIN" ]]; then
                    sql_query+=" (i.${dimname} < o.${dimname} AND i.${dimname} IS NOT NULL AND o.${dimname} IS NOT NULL)"
                elif [[ $minmax == "MAX" ]]; then
                    sql_query+=" (i.${dimname} > o.${dimname} AND i.${dimname} IS NOT NULL AND o.${dimname} IS NOT NULL)"
                fi

                if [[ $i != ${dimension} ]]; then
                    sql_query+=" OR\n"
                fi
            done

            sql_query+="\n  )\n)\n"

            echo -en $sql_query > $creation_path
            sed -i 's/\\\*/\*/g' $creation_path

            echo "ok"

            echo -n "running query ... "

            output_path=$(realpath ${output_folder}/${filename}.out)

            touch ${output_path}

            /.${SPARK_HOME}/bin/spark-sql \
                ${run_args} \
                --num-executors ${nodes} \
                -f ${absolute_path} \
                &> ${output_path}

            echo "ok"
        done
    done

    for algorithm in ${algorithms[@]}
    do
        for dataset in ${datasets_complete[@]}
        do
            for dimension in ${num_dimensions[@]}
            do

                filename=${algorithm}-${dataset}-${tuples[${dataset}]}t-${dimension}d-${nodes}n.sql
                creation_path=${destination_folder}/${filename}
                absolute_path=$(realpath ${creation_path})

                dimlist=(${dimensions[${dataset}]})
                minmaxlist=(${minmaxdiff[${dataset}]})

                echo -n "creating ${filename} ... "
                
                touch $creation_path

                sql_query="SELECT \* FROM ${database}.${dataset} SKYLINE OF "

                if [[ $algorithm == "bnl" ]]; then
                    sql_query+="BNL\n"
                elif [[ $algorithm == "dist" ]]; then
                    sql_query+="COMPLETE\n"
                elif [[ $algorithm == "dist_inc" ]]; then
                    sql_query+="\n"
                fi

                for i in $(seq 1 ${dimension}); do
                    dimname="${dimlist[${i}-1]}"
                    minmax="${minmaxlist[${i}-1]}"

                    if [[ $i == ${dimension} ]]; then
                        sql_query+="${dimname} ${minmax}\n"
                    else
                        sql_query+="${dimname} ${minmax},\n"
                    fi
                done

                echo -en $sql_query > $creation_path
                sed -i 's/\\\*/\*/g' $creation_path

                echo "ok"

                echo -n "running query ... "

                output_path=$(realpath ${output_folder}/${filename}.out)

                touch ${output_path}

                /.${SPARK_HOME}/bin/spark-sql \
                    ${run_args} \
                    --num-executors ${nodes} \
                    -f ${absolute_path} \
                    &> ${output_path}

                echo "ok"
            done
        done
    done

    for dataset in ${datasets_incomplete[@]}
    do
        for dimension in ${num_dimensions[@]}
        do
            filename=dist_inc-${dataset}-${tuples[${dataset}]}t-${dimension}d-${nodes}n.sql
            creation_path=${destination_folder}/${filename}
            absolute_path=$(realpath ${creation_path})

            dimlist=(${dimensions[${dataset}]})
            minmaxlist=(${minmaxdiff[${dataset}]})

            echo -n "creating ${filename} ... "
            
            touch $creation_path

            sql_query="SELECT \* FROM ${database}.${dataset} SKYLINE OF\n"

            for i in $(seq 1 ${dimension}); do
                dimname="${dimlist[${i}-1]}"
                minmax="${minmaxlist[${i}-1]}"

                if [[ $i == ${dimension} ]]; then
                    sql_query+="${dimname} ${minmax}\n"
                else
                    sql_query+="${dimname} ${minmax},\n"
                fi
            done

            echo -en $sql_query > $creation_path
            sed -i 's/\\\*/\*/g' $creation_path

            echo "ok"

            echo -n "running query ... "

            output_path=$(realpath ${output_folder}/${filename}.out)

            touch ${output_path}

            /.${SPARK_HOME}/bin/spark-sql \
                ${run_args} \
                --num-executors ${nodes} \
                -f ${absolute_path} \
                &> ${output_path}

            echo "ok"
        done
    done
done
