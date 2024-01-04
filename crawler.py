import os
import time
import json
import argparse
import pandas as pd
import requests as rq
import multiprocessing as mp

from tqdm import tqdm
from pprint import pprint as pp

from typing import List

from json_decoder import json_decoder
from multiprocessing.pool import Pool


class Crawler:
    def __init__(self, save_path: str, file_name: str, start_year: int, end_year: int,
                 headers: dict, url: str, lang: str, process_counter: int) -> None:
        self.__save_path = save_path
        self.__file_name = file_name
        self.__start_year = start_year
        self.__end_year = end_year

        self.__url = url
        self.__lang = lang
        self.__headers = headers
        self.__process_counter = process_counter

    # Set-Get
    @property
    def save_path(self):
        return self.__save_path

    @save_path.setter
    def save_path(self, value):
        self.__save_path = value

    @property
    def start_year(self):
        return self.__start_year

    @start_year.setter
    def start_year(self, value):
        self.__start_year = value

    @property
    def end_year(self):
        return self.__end_year

    @end_year.setter
    def end_year(self, value):
        self.__end_year = value

    @property
    def lang(self):
        return self.__lang

    @lang.setter
    def lang(self, value):
        self.__lang = value

    @property
    def url(self):
        return self.__url

    @url.setter
    def url(self, value):
        self.__url = value

    @property
    def file_name(self):
        return self.__file_name

    @file_name.setter
    def file_name(self, value):
        self.__file_name = value

    @property
    def headers(self):
        return self.__headers

    @headers.setter
    def headers(self, value):
        self.__headers = value

    @property
    def process_counter(self):
        return self.__process_counter

    @process_counter.setter
    def process_counter(self, process_counter: int):
        self.__process_counter = process_counter


    # Support methods
    def __GetTotalPages(self, year) -> int:
        # Get total pages for each year
        # page should be range from 1 to 500
        url = self.__url + self.__lang + "year=" + str(year)

        total_pages = rq.get(url, headers=self.__headers).json()['total_pages']
        total_pages = min(total_pages, 500)
        return total_pages


    # Main Methods
    def MetadataCrawler(self):
        """
        Returns: {obj1},
                 {obj2},
                 {obj3}

        Appropriate JSON format: [{obj1},
                                  {obj2},
                                  {obj3}]
        => Don't forget to add "[" and "]" at the beginning and end of file after crawling procedure finished
        """
        # write each process into separate file and merge 'em later
        file_prefix = mp.current_process().name.split("-")[1]
        file = os.path.join(self.__save_path, f"{file_prefix}_{self.__file_name}.{options.file_extension}")

        # start crawling
        for year in tqdm(range(self.__start_year, self.__end_year), position=self.__process_counter,
                         desc=f"Process: {file_prefix:3s} {self.__start_year} to {self.__end_year}", colour='white'):
            total_pages = self.__GetTotalPages(year)

            # Crawl page by page -- 1 page comprises 20 results
            for page in range(1, total_pages + 1):
                url = self.__url + self.__lang + "year=" + str(year) + "&" + "page=" + str(page)
                response = rq.get(url, headers=options.headers)

                # Test "results" field is available or not
                try:
                    result: list = response.json()["results"]
                    result: List[str] = [json.dumps(json_object, indent=4) for json_object in result]
                    # save to file
                    if len(result) != 0:
                        with open(file=file, mode="a", encoding="UTF-8") as f:
                            for json_object in result:
                                f.write(json_object + ",\n")
                except Exception as e:
                    print(e)
        return None


def execute_metadata_crawling(save_path:str, file_name: str, start_year: int, end_year: int,
                              headers: dict, url: str, lang: str, process_counter) -> None:
    crawler = Crawler(save_path, file_name, start_year, end_year, headers, url, lang, process_counter)
    crawler.MetadataCrawler()
    return None


def set_up_metadata_crawling() -> None:
    start_time = time.time()
    global options

    # Init
    process_counter = 0

    lower_bound = options.start_year
    interval = (options.end_year - options.start_year) // (options.num_of_processes - 1)  # last process handles remaining years

    pool = Pool(processes=options.num_of_processes)
    save_path = os.path.join(options.data_path, "metadata")

    # map to multiprocesses
    configurations = []
    while lower_bound + interval < options.end_year:
        # remaining years will be handled after this while loop
        configurations.append((save_path, options.file_name, lower_bound, lower_bound + interval,
                               options.headers, options.url, options.lang, process_counter))
        # Update
        lower_bound += interval
        process_counter += 1

    # Handle remaining years
    configurations.append((save_path, options.file_name, lower_bound, options.end_year,
                           options.headers, options.url, options.lang, process_counter))

    # creates multiprocesses in pool
    pool.starmap(func=execute_metadata_crawling, iterable=configurations)
    print("Finished crawl in:", time.time() - start_time)
    return None


def set_up_movie_detail_crawling(metadata_path: str) -> None:
    global options

    pool = Pool(processes=options.num_of_processes)
    options.file_name = os.path.join(options.save_path, f"{options.file_name}.{options.file_extension}")

    with open(metadata_path, "r", encoding="UTF-8") as f:
        df = json.loads(f.read())
        df = pd.json_normalize(df)

        # df = pd.read_json(json.dumps(metadata_path), orient="records", typ="records")
        print(df)

    # df = pd.json_normalize(df)

    # json.decoder.JSONDecodeError: Extra data: line 23 column 2 (char 756)
    # Solution ref: https://bobbyhadz.com/blog/python-jsondecodeerror-extra-data

    # df = json.load(f)
    # print(df)

    # print(df)


parser = argparse.ArgumentParser()
parser.add_argument("--lang", type=str, default="language=en-US&")
parser.add_argument("--headers", type=dict, default={"accept": "application/json",
                                                     "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJkMjFhODEzOTg5MWY0NDU0YmI3MmMwOTRkZjk4MjMxMSIsInN1YiI6IjY0YWUyMTE2M2UyZWM4MDBhZjdmOTI5NiIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.u85xU7i1cX_jR69x4OBq24kDtOIdvpK3FbYLffwBWSU"})
parser.add_argument("--start_year", type=int, default=1920)
parser.add_argument("--end_year", type=int, default=2024)
parser.add_argument("--num_of_processes", type=int, default=25)
parser.add_argument("--data_path", type=str, default=os.path.join(os.getcwd(), "data", "raw_data"))
parser.add_argument("--file_extension", type=str, default="json")
# url, file_name: depend on what will be crawled
parser.add_argument("--url", type=str)
parser.add_argument("--file_name", type=str)
options = parser.parse_args()


def merge_tmp_file(dir: str, delete_tmp_file=False):
    global options
    path = os.path.join(options.data_path, "raw_data", dir)
    file_lst = os.listdir(path)
    file_name = f"{options.file_name}.{options.file_extension}"

    # remove metadata.json if it exists
    if file_name in file_lst:
        file_lst.remove(file_name)
        os.remove(os.path.join(path, file_name))
        print("metadata.json has been deleted")

    # Merge into one file
    with open(file=os.path.join(path, f"{options.file_name}.{options.file_extension}"), mode="w", encoding="UTF-8") as writer:
        for i in range(len(file_lst)):
            with open(file=os.path.join(path, file_lst[i]), mode="r", encoding="UTF-8") as reader:
                for obj in next(json_decoder(reader.read())):
                    writer.write(json.dumps(obj, indent=4) + "\n")
            if i == 1:
                print(i, " file has been written")
            else:
                print(i, " file have been written")

    # Delete temp files
    if delete_tmp_file:
        for file in file_lst:
            os.remove(os.path.join(path, file))
        print("Temp files have been removed")

def main() -> None:
    # Metadata crawling
    options.url = "https://api.themoviedb.org/3/discover/movie?include_adult=false&include_video=false&"
    options.file_name = "metadata"
    set_up_metadata_crawling()
    merge_tmp_file(dir="metadata")

    # Movie detail crawling
    # metadata_path = os.path.join(options.save_path, f"{options.file_name}.{options.file_extension}")
    # options.url = "https://api.themoviedb.org/3/movie/"
    # options.file_name = "movie_detail"
    # set_up_movie_detail_crawling(metadata_path)
    return None


if __name__ == '__main__':
    main()
