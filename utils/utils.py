import os
import json
import pandas as pd
from pprint import pp
from json_decoder import json_decoder


def merge_file(data_path: str, file_name: str, delete_tmp_file=False) -> None:
    file_path = os.path.join(data_path, file_name)
    file_lst = os.listdir(file_path)

    # remove metadata.json if it exists
    if f"{file_name}.json" in file_lst:
        file_lst.remove(f"{file_name}.json")
        os.remove(os.path.join(file_path, f"{file_name}.json"))
        print("metadata.json has been deleted")

    # Merge into one file
    with open(file=os.path.join(file_path, f"{file_name}.json"), mode="w", encoding="UTF-8", errors='ignore') as writer:
        writer.write("[")

        for i in range(len(file_lst)):
            with open(file=os.path.join(file_path, file_lst[i]), mode="r", encoding="UTF-8", errors='ignore') as reader:
                # Data type flow: string -> string -> obj -> string
                data = reader.read()[:-2] # remove last line and last comma
                data = json.dumps(data)
                # writer.write(next(json_decoder(json.dumps(reader.read()))) + ",\n")
                for obj in json_decoder(data):
                    writer.write(obj)

            print(i, " file has been written") if i == 1 else print(i, " file have been written")

            if i < (len(file_lst) - 1):
                writer.write(",")

        writer.write("]")

    # Delete temp files
    if delete_tmp_file:
        for file in file_lst:
            os.remove(os.path.join(file_path, file))
        print("Temp files have been removed")
    return None



def en_movie_filtering(file: str) -> None:
    with open(file=file, mode="r", encoding="UTF-8", errors="ignore`") as f:
        data = json_decoder(f.read())
        df = pd.DataFrame(next(data))

        df = df[df["original_language"] == "en"]
        df = df.drop(labels="original_language", axis="columns")
        df = df.drop_duplicates(subset="id", keep="first")

        df.to_json(path_or_buf=file, orient="records", indent=4)
        print("Filtering finished")
    return None


def count_movies_by_lang(file: str) -> None:
    """
        This function must run prior to merging files
    """
    with open(file=file, mode="r", encoding="UTF-8", errors="ignore`") as f:
        data = json_decoder(f.read())
        df = pd.DataFrame(next(data))
        df = df.drop_duplicates(subset="id", keep="first")
        result = df.groupby(by="original_language", axis="index").count()
        pp(sorted(result.adult.to_dict().items(), key=lambda item: item[1], reverse=True))

def count_en_movies(file: str) -> int:
    with open(file=file, mode="r", encoding="UTF-8", errors="ignore`") as f:
        data = json_decoder(f.read())
        df = pd.DataFrame(next(data))
        return len(df)



