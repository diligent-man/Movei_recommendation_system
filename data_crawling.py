import os
import time
import argparse
import pandas as pd
import requests as rq
from tqdm import tqdm
from pprint import pprint as pp
from multiprocessing.pool import Pool

class Crawler:
    def __init__(self, file_name: str, start_year: int, end_year: int,
                 headers: dict, url: str, lang: str, process_counter: int) -> None:
        self.__file_name = file_name
        self.__start_year = start_year
        self.__end_year = end_year
        self.__headers = headers
        self.__url = url
        self.__lang = lang
        self.__process_counter = process_counter

    # Set-Get
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
        total_pages = min(total_pages, 500)  # ???
        return total_pages

    def __GetFields(self, year: int) -> list:
        url = self.__url + self.__lang + "year=" + str(year)
        fields = rq.get(url, headers=self.__headers).json()["results"][0].keys()
        return fields

    def __FieldsWriting(self, year: int = 1920) -> None:
        # Write header to output file
        fields = self.__GetFields(year)
        df = pd.DataFrame(columns=fields)
        df.to_csv(self.__file_name, mode='w', index=False)
        return None

    def Crawl(self):
        if options.file_extension == "csv":
            self.__FieldsWriting()

        # Start crawling
        for year in tqdm(range(self.__start_year, self.__end_year), position=self.__process_counter,
                         desc=f"{self.__start_year} to {self.__end_year}", colour='white'):
            total_pages = self.__GetTotalPages(year)

            for page in range(1, total_pages+1):
                url = self.__url + self.__lang + "year=" + str(year) + "&" + "page=" + str(page)

                response = rq.get(url, headers=self.__headers)

                try:
                    result = response.json()['results']
                    # save to file
                    if options.file_extension == "json":
                        pd.DataFrame(result).to_json(self.__file_name, mode='a', orient="records", indent=4, lines=True)
                    elif options.file_extension == "csv":
                        pd.DataFrame(result).to_csv(self.__file_name, mode='a', header=False, index=False)
                except:
                    continue
        return None

def execute_crawling(file_name: str, start_year: int, end_year: int,
                     headers: dict, url: str, lang: str, process_counter) -> None:
    crawler = Crawler(file_name, start_year, end_year, headers, url, lang, process_counter)
    crawler.Crawl()


def set_up_crawling() -> None:
    pool = Pool(processes=options.num_of_processes)
    options.file_name = os.path.join(options.save_path, 'raw_data', f"metadata.{options.file_extension}")

    process_counter = 0
    lower_bound = options.start_year
    interval = (options.end_year - options.start_year) // options.num_of_processes

    # map to multiprocesses
    configurations = []
    while lower_bound + interval < options.end_year:
        # remaining years will be handled after this while loop
        configurations.append((options.file_name, lower_bound, lower_bound+interval,
                               options.headers, options.url, options.lang, process_counter))
        # Update
        lower_bound += interval
        process_counter += 1

    # Handle remaining years
    configurations.append((options.file_name, lower_bound, options.end_year,
                           options.headers, options.url, options.lang, process_counter))

    # creates multiprocesses in pool
    pool.starmap(func=execute_crawling, iterable=configurations)
    return None


parser = argparse.ArgumentParser()
parser.add_argument("--lang", type=str, default="language=en-US&")
parser.add_argument("--headers", type=dict, default={"accept": "application/json","Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJkMjFhODEzOTg5MWY0NDU0YmI3MmMwOTRkZjk4MjMxMSIsInN1YiI6IjY0YWUyMTE2M2UyZWM4MDBhZjdmOTI5NiIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.u85xU7i1cX_jR69x4OBq24kDtOIdvpK3FbYLffwBWSU"})
parser.add_argument("--start_year", type=int, default=1920)
parser.add_argument("--end_year", type=int, default=2024)
parser.add_argument("--num_of_processes", type=int, default=50)
parser.add_argument("--save_path", type=str, default=os.path.join(os.getcwd(), "data"))
parser.add_argument("--file_extension", type=str, default="json")
parser.add_argument("--url", type=str)
parser.add_argument("--file_name", type=str)
options = parser.parse_args()
def main() -> None:
    metadata = {"url": ["https://api.themoviedb.org/3/discover/movie?include_adult=false&include_video=false&",
                        "https://api.themoviedb.org/3/movie/"],
                "file_name": ["metadata", "movie_detail"]
                }
    # for i in range(len(metadata["url"])):
    for i in range(1):
        options.url = metadata["url"][i]
        options.file_name = metadata["file_name"][i]
        set_up_crawling()
    return None


if __name__ == '__main__':
    main()