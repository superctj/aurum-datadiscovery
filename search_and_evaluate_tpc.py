import argparse
import logging
import os
import sys

import pandas as pd

from algebra import API
from main import init_system

sys.path.append("/home/tianji/lake-graph-view")
from src.utils.custom_logging import LOGGING_LEVELS, setup_logging


def collect_ground_truth(ground_truth_filepath: str) -> set:
    ground_truth_df = pd.read_csv(ground_truth_filepath)
    ground_truth = set()
    
    for _, row in ground_truth_df.iterrows():
        left_table = row["fk_table"]
        left_column = row["fk_column"]
        right_table = row["pk_table"]
        right_column = row["pk_column"]
        ground_truth.add((left_table, left_column, right_table, right_column))
    
    return ground_truth


def check_ground_truth(ground_truth: set, query_table: str, query_column: str, candidate_table: str, candidate_column: str) -> bool:
    return (query_table, query_column, candidate_table, candidate_column) in ground_truth or (candidate_table, candidate_column, query_table, query_column) in ground_truth


def tpc_search_and_eval(
    aurum_api: API,
    csv_dir: str,
    aurum_ds_name: str,
    ground_truth: set
):
    logger = logging.getLogger(__name__)

    num_queries = 0
    num_tp = 0
    num_fp = 0

    # Use columns with unique values as queries
    for filename in os.listdir(csv_dir):
        if not filename.endswith(".csv"):
            continue

        table_path = os.path.join(csv_dir, filename)
        df = pd.read_csv(table_path, sep=",")
        table_name = filename[:-4]  # Remove .csv extension

        for col_name in df.columns:
            if df[col_name].nunique() == len(df):
                logger.info(f"\nUsing {table_name}.{col_name} as query column.")

                field = (aurum_ds_name, filename, col_name)
                drs = aurum_api.make_drs(field)
                
                try:
                    results = aurum_api.pkfk_of(drs)
                except KeyError as e:
                    logger.error(f"Key Error for {table_name}.{col_name}: {e}")
                    exit(1)

                num_queries += 1

                for res in results:
                    candidate_table_name = res[2][:-4]  # Remove .csv extension
                    candidate_col_name = res[3]
                    logger.info(
                        f"Candidate table: {candidate_table_name}; Candidate column: {candidate_col_name}"
                    )

                    if candidate_table_name == table_name and candidate_col_name == col_name:
                        logger.warning("Found self-match. Skipping...")
                        continue

                    is_correct = check_ground_truth(
                        ground_truth,
                        table_name,
                        col_name,
                        candidate_table_name,
                        candidate_col_name
                    )

                    if is_correct:
                        num_tp += 1
                    else:
                        num_fp += 1

    precision = num_tp / (num_tp + num_fp) if (num_tp + num_fp) > 0 else 0
    recall = num_tp / len(ground_truth) if len(ground_truth) > 0 else 0
    f1 = (2 * precision * recall) / (precision + recall) if (precision + recall) > 0 else 0

    logger.info(f"# queries: {num_queries}")
    logger.info(f"F1 score: {f1:.2f}")
    logger.info(f"Precision: {precision:.2f}")
    logger.info(f"Recall: {recall:.2f}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--aurum_model_dir",
        type=str,
        default="/home/tianji/aurum-datadiscovery/aurum_models/tpcds/",
        help="Directory containing the Aurum model"
    )

    parser.add_argument(
        "--aurum_ds_name",
        type=str,
        default="tpcds_csv_repository",
        help="Aurum data source name"
    )

    parser.add_argument(
        "--csv_dir",
        type=str,
        default="/home/tianji/lake-graph-view/artifacts/aurum/tpcds/tables_csv",
        help="Directory containing the TPC CSV files"
    )

    parser.add_argument(
        "--ground_truth_filepath",
        type=str,
        default="/home/tianji/lake-graph-view/artifacts/tpc_ds_sf_1/fk_pk.csv",
        help="Path to ground truth file for evaluation"
    )

    parser.add_argument(
        "--output_dir",
        type=str,
        default="/home/tianji/lake-graph-view/artifacts/aurum/tpcds/search_results",
        help="Directory to save the output files"
    )

    parser.add_argument(
        "--logging_dir",
        type=str,
        default="/home/tianji/lake-graph-view/logs/aurum/tpcds/search_and_evaluation",  # noqa: E501
        help="Directory to save logs",
    )

    parser.add_argument(
        "--logging_level",
        type=str,
        default="info",
        choices=LOGGING_LEVELS.keys(),
        help="Logging level",
    )

    args = parser.parse_args()

    setup_logging(args.logging_dir)
    logger = logging.getLogger(__name__)
    logger.setLevel(LOGGING_LEVELS[args.logging_level])

    logger.info(f"Running program: {__file__}\n")
    logger.info(f"Arguments: {args}\n")

    aurum_api, reporting = init_system(args.aurum_model_dir)
    ground_truth = collect_ground_truth(args.ground_truth_filepath)
    tpc_search_and_eval(
        aurum_api=aurum_api,
        csv_dir=args.csv_dir,
        aurum_ds_name=args.aurum_ds_name,
        ground_truth=ground_truth
    )
