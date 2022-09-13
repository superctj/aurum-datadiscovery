import argparse
import logging
import os
import shutil
import sys
from typing import Tuple

from tqdm import tqdm

from algebra import API
from main import init_system
sys.path.append("/home/ubuntu/join_discovery/")
from util.data_loader import CSVDataLoader, SpiderCSVDataLoader
from util.logging import custom_logger, log_args_and_metrics, log_query_and_answer, log_search_results


def create_new_directory(path: str, force: bool = False):
    if not os.path.exists(path):
        os.makedirs(path)
    else:
        if force:
            shutil.rmtree(path)
            os.makedirs(path)
        else:
            raise FileNotFoundError


def aurum_search_and_eval(aurum_api: API, dataloader: CSVDataLoader, aurum_ds_name: str, output_dir: str, k: int) -> Tuple[float, float]:
    queries = dataloader.get_queries()
    
    precision, recall = [], []
    num_queries = 0

    for query_id in tqdm(queries.keys()):
        # print("Query: ", query_id)
        # exit()
        db_table_name, query_column_name = query_id.split("!")
        query_table_name = "+".join(db_table_name.split("/"))
        # print("Query table name: ", query_table_name)
        # exit()
        # query_table = dataloader.read_table(table_name=query_table_name)
        # query_column = query_table[query_column_name]

        # # Skip numerical column
        # if query_column.dtype != "object":
        #     continue
        
        output_file = os.path.join(output_dir, f"q{num_queries+1}.txt")
        logger = custom_logger(output_file)
        log_query_and_answer(logger, query_table_name, query_column_name, answers=queries[query_id])

        field = (aurum_ds_name, f"{query_table_name}.csv", query_column_name)
        drs = aurum_api.make_drs(field)
        try:
            results = aurum_api.content_similar_to(drs)
        except KeyError:
            print("Key Error")
            print("Table name: ", query_table_name)
            print("Column name: ", query_column_name)
            print("=" * 50)
            continue

        num_corr = 0
        result_size = results.size()

        for i, res in enumerate(results):
            if i >= k: break

            candidate_table_name = res[2][:-4]
            candidate_table_name = "/".join(candidate_table_name.split("+"))
            candidate_column_name = res[3]
            candidate_id = candidate_table_name + "!" + candidate_column_name
            # print("Candidate ID: ", candidate_id)
            # print("Answers: ", queries[query_id])
            # exit()

            if candidate_id == query_id:
                raise ValueError("Aurum returns query itself as answer!")

            if candidate_id in queries[query_id]:
                num_corr += 1

            log_search_results(logger, candidate_table_name, candidate_column_name, score=res[4])
        
        # Aurum stores relationship in a graph and does not support top-k search
        # if k <= result_size or result_size == 0:
        #     local_k = k
        # else:
        #     local_k = result_size
        precision.append(num_corr / k)
        recall.append(num_corr / len(queries[query_id]))
        num_queries += 1

    avg_precision = sum(precision) / num_queries
    avg_recall = sum(recall) / num_queries
    return avg_precision, avg_recall, num_queries


def main(args: argparse.Namespace):
    # Load aurum model
    # model_dir_path = "/data/elasticsearch/spider/testmodel/"
    # model_dir_path = "/data/aurum_elasticsearch/nextiaJD-L/testmodel/"
    api, reporting = init_system(args.aurum_model_dir)

    # Get queries and ground truth
    dataloader = SpiderCSVDataLoader(
        dataset_dir=args.dataset_dir, 
        metadata_path=args.metadata_path,
    )

    output_dir = os.path.join(
        args.output_dir, f"topk_{args.top_k}"
    )
    create_new_directory(output_dir, force=True)

    # Query and evaluation
    metrics = aurum_search_and_eval(api, dataloader, args.aurum_ds_name, output_dir, args.top_k)

    # Log command-line arguments for reproduction and metrics
    meta_log_file = os.path.join(output_dir, "log.txt")
    meta_logger = custom_logger(meta_log_file, level=logging.INFO)
    log_args_and_metrics(meta_logger, args, metrics)


def _aurum_play_around():
    # Load aurum model
    model_dir_path = "/data/aurum_elasticsearch/nextiaJD-L/testmodel/" # "/data/elasticsearch/spider/testmodel/"

    # Init aurum
    api, reporting = init_system(model_dir_path)
    
    # Query
    field = ("nextiaJD-l_csv_repository", "Parking_Violations_Issued_Fiscal_Year_2016.csv", "Registration State")
    drs = api.make_drs(field)
    res = api.content_similar_to(drs)

    print("RES size: " + str(res.size()))
    for el in res:
        print(el[2], el[3], el[4])


if __name__ == "__main__":
    # _aurum_play_around()
    parser = argparse.ArgumentParser(description="Aurum Top-K Search",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("--aurum_ds_name", type=str, default="", help="")
    parser.add_argument("--aurum_model_dir", type=str, default="", help="")
    parser.add_argument("--dataset_dir", type=str, default="", help="")
    parser.add_argument("--metadata_path", type=str, default="", help="")
    parser.add_argument("--output_dir", type=str, default="", help="")
    parser.add_argument("--top_k", type=int, default=41, help="")
    
    main(parser.parse_args())