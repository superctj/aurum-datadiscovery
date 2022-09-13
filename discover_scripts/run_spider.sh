#!/bin/bash
set -e

aurum_ds_name="spider_csv_repository"
aurum_model_dir="/data/aurum_elasticsearch/spider/testmodel/"
partition="dev"
#k=2

for k in 2 3 5 10
do
    python discovery_spider.py \
    --aurum_ds_name="${aurum_ds_name}" \
    --aurum_model_dir="${aurum_model_dir}" \
    --dataset_dir="/data/spider_artifact/db_csv_extended_flat/" \
    --metadata_path="/data/spider_artifact/${partition}_join_data_extended.csv" \
    --output_dir="./outputs/spider_${partition}/" \
    --top_k=${k}
done