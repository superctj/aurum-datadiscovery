#!/bin/bash
set -e

testbed="testbedM"
aurum_ds_name="nextiaJD_${testbed}_csv_repository"
aurum_model_dir="/data/aurum_elasticsearch/nextiaJD/${testbed}/aurum_model/"
#k=2

for k in 2 3 5 10
do
    python discovery_nextiaJD.py \
    --aurum_ds_name="${aurum_ds_name}" \
    --aurum_model_dir="${aurum_model_dir}" \
    --dataset_dir="/data/nextiaJD_datasets/${testbed}/datasets/" \
    --metadata_path="/data/nextiaJD_datasets/${testbed}/datasetInformation_${testbed}.csv" \
    --ground_truth_path="/data/nextiaJD_datasets/${testbed}/groundTruth_${testbed}.csv" \
    --output_dir="./outputs/${testbed}/" \
    --top_k=${k}
done