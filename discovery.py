from main import init_system


if __name__ == "__main__":
    model_dir_path = "/data/elasticsearch/testmodel/"
    api, reporting = init_system(model_dir_path)

    field = ("spider_csv_repository", "concert_singer+concert.csv", "name")
    drs = api.make_drs(field)

    res = api.content_similar_to(drs)

    print("RES size: " + str(res.size()))
    for el in res:
        print(str(el))