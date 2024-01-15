import os
import sys
sys.path.append("utils")

import json
import pandas as pd
from json_decoder import json_decoder


def merge_file(data_path, file_name, delete_tmp_file=False) -> None:
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

                # writer.write(next(json_decoder(json.dumps(reader.read()))) + ",\n")
                print(next(json_decoder(json.dumps(reader.read()))) + ",\n")

            if i == 1:
                print(i, " file has been written")
            else:
                print(i, " file have been written")
            break
        writer.write("]")

    # Delete temp files
    if delete_tmp_file:
        for file in file_lst:
            os.remove(os.path.join(file_path, file))
        print("Temp files have been removed")
    return None


def en_movie_filtering(file: str) -> None:
    with open(file=file, mode="r", encoding="UTF-8", errors="ignore`") as f:
        df = pd.DataFrame(json_decoder(f.read()))
        df = df[df["original_language"] == "en"]
        df = df.drop(labels="original_language", axis="columns")
        print(df)
        df = df.drop_duplicates(subset="id", keep="first")
        print(df)
        df.to_json(path_or_buf=file, orient="records", indent=4)
    return None