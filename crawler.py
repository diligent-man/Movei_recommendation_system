import os
import time
import json
import argparse
from argparse import Namespace

import pandas as pd
import requests as rq
import multiprocessing as mp

from tqdm import tqdm
from pprint import pprint as pp

from typing import List, Tuple, Any

from json_decoder import json_decoder
from multiprocessing.pool import Pool


class Crawler:
    def __init__(self, start_year: int, end_year: int,
                       headers: dict, lang: str, process_counter: int,  file_extension: str,
                       metadata_save_path: str, movie_detail_save_path: str,
                       metadata_file_name: str, movie_detail_file_name: str,
                       metadata_url: str, movie_detail_url: str
                 ) -> None:
        self.__start_year = start_year
        self.__end_year = end_year
        self.__headers = headers
        self.__lang = lang
        self.__process_counter = process_counter
        self.__file_extension = file_extension

        self.__metadata_save_path = metadata_save_path
        self.__metadata_file_name = metadata_file_name
        self.__metadata_url = metadata_url

        self.__movie_detail_save_path = movie_detail_save_path
        self.__movie_detail_file_name = movie_detail_file_name
        self.__movie_detail_url = movie_detail_url


    # Support methods
    def __GetTotalPages(self, year) -> int:
        # Get total pages for each year
        # page should be range from 1 to 500
        url = self.__metadata_url + self.__lang + "year=" + str(year)

        total_pages = rq.get(url, headers=self.__headers).json()['total_pages']
        total_pages = min(total_pages, 500)
        return total_pages


    # Main Methods
    def Crawl(self) -> None:
        """
        Returns data format: {obj1},
                             {obj2},
                             {obj3}

        Appropriate JSON format: [{obj1},
                                  {obj2},
                                  {obj3}]
        => Don't forget to add "[" and "]" at the beginning and end of file after merge files
        """

        # write each process into separate file and merge 'em later
        prefix = mp.current_process().name.split("-")[1]
        metadata_file_name = os.path.join(self.__metadata_save_path, f"{prefix}_{self.__metadata_file_name}.{self.__file_extension}")
        movie_detail_file_name = os.path.join(self.__movie_detail_save_path, f"{prefix}_{self.__movie_detail_file_name}.{self.__file_extension}")

        # start crawling
        for year in tqdm(range(self.__start_year, self.__end_year),
                         position=self.__process_counter,
                         desc=f"Process: {prefix:3s} {self.__start_year} to {self.__end_year}", colour='white'
                         ):
            total_pages = self.__GetTotalPages(year)

            # Crawl page by page -- 1 page comprises 20 results
            for page in range(1, total_pages + 1):
                metadata_url = self.__metadata_url + self.__lang + "year=" + str(year) + "&" + "page=" + str(page)
                metadata_response = rq.get(metadata_url, headers=self.__headers)
                time.sleep(.5)
                # Test "results" field is available or not
                try:
                    meta_data_result: list = metadata_response.json()["results"]
                    meta_data_json_str: List[str] = [json.dumps(json_object, indent=4) for json_object in meta_data_result]

                    # save to file
                    if len(meta_data_json_str) != 0:
                        with open(file=metadata_file_name, mode="a", encoding="UTF-8") as f:
                            for json_object in meta_data_json_str:
                                f.write(json_object + ",\n")


                    # crawl movie detail
                    id = meta_data_result[0]["id"]
                    movie_detail_url = self.__movie_detail_url + str(id) + "?" + self.__lang
                    movie_detail_response = rq.get(movie_detail_url, headers=self.__headers)
                    time.sleep(.5)
                    movie_detail_json_str: dict = movie_detail_response.json()
                    movie_detail_json_str: str = json.dumps(movie_detail_json_str)

                    # save to file
                    # don't need try catch cuz data is always available
                    if len(movie_detail_json_str) != 0:
                        with open(file=movie_detail_file_name, mode="a", encoding="UTF-8") as f:
                            f.write(movie_detail_json_str + ",\n")
                except Exception as e:
                    print(e)
        return None


def execute_crawling(start_year: int, end_year: int,
                     headers: dict, lang: str, process_counter: int, file_extension: str,
                     metadata_save_path: str, movie_detail_save_path: str,
                     metadata_file_name: str, movie_detail_file_name: str,
                     metadata_url: str, movie_detail_url: str):

    crawler = Crawler(start_year, end_year,
                      headers, lang, process_counter, file_extension,
                      metadata_save_path, movie_detail_save_path,
                      metadata_file_name, movie_detail_file_name,
                      metadata_url, movie_detail_url)
    crawler.Crawl()
    return None


def set_up_crawling(options: dict) -> None:
    # For multiprocessing
    pool = Pool(processes=options.num_of_processes+1)
    process_counter = 0

    # For movie_detail
    movie_detail_save_path = os.path.join(options.data_path, "movie_detail")

    # For metadata
    metadata_save_path = os.path.join(options.data_path, "metadata")

    lower_bound = options.start_year
    interval = (options.end_year - options.start_year) // options.num_of_processes  # last process handled by the first process             options.end_year - options.start_year) // options.num_of_processes  # last process handled by the first process

    # Map to multiprocesses
    configurations = []
    while lower_bound + interval < options.end_year:
        # remaining years will be handled after this while loop
        configurations.append((lower_bound, lower_bound + interval,
                               options.headers, options.lang, process_counter, options.file_extension,
                               metadata_save_path, movie_detail_save_path,
                               options.metadata_file_name, options.movie_detail_file_name,
                               options.metadata_url, options.movie_detail_url))

        # Update boundary for metadata crawling
        lower_bound += interval
        process_counter += 1

    # Handle remaining years
    configurations.append((lower_bound, options.end_year,
                           options.headers, options.lang, process_counter, options.file_extension,
                           metadata_save_path, movie_detail_save_path,
                           options.metadata_file_name, options.movie_detail_file_name,
                           options.metadata_url, options.movie_detail_url))
    # Check configs
    # pp(configurations)

    # creates multiprocesses in pool
    pool.starmap(func=execute_crawling, iterable=configurations)
    return None


def merge_file(data_path, file_extension, file_name, delete_tmp_file=False) -> None:
    file_path = os.path.join(data_path, file_name)
    file_lst = os.listdir(file_path)
    file_name_with_extension = f"{file_name}.{file_extension}"

    # remove metadata.json if it exists
    if file_name_with_extension in file_lst:
        file_lst.remove(file_name_with_extension)
        os.remove(os.path.join(file_path, file_name_with_extension))
        print("metadata.json has been deleted")

    # Merge into one file
    with open(file=os.path.join(file_path, file_name_with_extension), mode="w", encoding="UTF-8", errors='ignore') as writer:
        for i in range(len(file_lst)):
            with open(file=os.path.join(file_path, file_lst[i]), mode="r", encoding="UTF-8", errors='ignore') as reader:
                for obj in next(json_decoder(reader.read())):
                    writer.write(json.dumps(obj, indent=4) + "\n")
                    continue

            if i == 1:
                print(i, " file has been written")
            else:
                print(i, " file have been written")

    # Delete temp files
    if delete_tmp_file:
        for file in file_lst:
            os.remove(os.path.join(file_path, file))
        print("Temp files have been removed")
    return None


def en_movie_filtering(options: dict, path: str) -> None:
    with open(file=path, mode="r", encoding="UTF-8", errors="ignore`") as f:
        df = pd.DataFrame(json_decoder(f.read()))
        df = df[df["original_language"] == "en"]
        df = df.drop(labels="original_language", axis="columns")
        df.to_json(path_or_buf=path, orient="records", indent=4)
    return None


def main() -> None:
    """
    This script is used to crawl data from TMDB including two procedures:
        + Metadata
        + Use id from metadata to crawl movie detail
    Therefore, 1 process will be used for crawl both
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--lang", type=str, default="language=en-US&")
    parser.add_argument("--headers", type=dict, default={"accept": "application/json",
                                                         "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJkMjFhODEzOTg5MWY0NDU0YmI3MmMwOTRkZjk4MjMxMSIsInN1YiI6IjY0YWUyMTE2M2UyZWM4MDBhZjdmOTI5NiIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.u85xU7i1cX_jR69x4OBq24kDtOIdvpK3FbYLffwBWSU"})
    parser.add_argument("--num_of_processes", type=int, default=50)
    parser.add_argument("--data_path", type=str, default=os.path.join(os.getcwd(), "data", "raw_data"))
    parser.add_argument("--file_extension", type=str, default="json")

    # For metadata
    parser.add_argument("--start_year", type=int, default=1920)
    parser.add_argument("--end_year", type=int, default=2024)
    parser.add_argument("--metadata_url", type=str, default="https://api.themoviedb.org/3/discover/movie?include_adult=false&include_video=false&")
    parser.add_argument("--metadata_file_name", type=str, default="metadata")

    # For movie detail
    parser.add_argument("--movie_detail_url", type=str, default="https://api.themoviedb.org/3/movie/")
    parser.add_argument("--movie_detail_file_name", type=str, default="movie_detail")

    options: Namespace = parser.parse_args()
    # set_up_crawling(options)


    # Merge files
    paras = [(options.data_path, options.file_extension, options.metadata_file_name),
             (options.data_path, options.file_extension, options.movie_detail_file_name)]

    para: Tuple[str, str, str]
    for para in paras:
        merge_file(*para, delete_tmp_file=False)


    # Select en movie in metadata & movie_detail
    paths = [os.path.join(options.data_path,
                          options.metadata_file_name,
                          f"{options.metadata_file_name}.{options.file_extension}"),
            os.path.join(options.data_path,
                         options.movie_detail_file_name,
                         f"{options.movie_detail_file_name}.{options.file_extension}")]
    for path in paths:
        en_movie_filtering(options, path)
    return None


if __name__ == '__main__':
    main()
