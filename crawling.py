import sys
import os
import argparse

from typing import Dict
from argparse import Namespace
from pprint import pprint as pp

from crawlers.MetadataCrawler import MetadataCrawler
from crawlers.MovieDetailCrawler import MovieDetailCrawler

from multiprocessor.multiprocessor import Multiprocessor
from utils.utils import count_en_movies, merge_file, en_movie_filtering


def metadata_crawler_init(start: int, end: int,
                          headers: dict, lang: str, url: str,
                          save_path: str, file_name: str, process_counter: int):
    crawler = MetadataCrawler(start, end, headers, lang, url, save_path, file_name, process_counter)
    crawler()
    return None


def movie_detail_crawler_init(start: int, end: int,
                              headers: dict, lang: str, url: str,
                              save_path: str, file_name: str, process_counter: int):
    crawler = MovieDetailCrawler(start, end, headers, lang, url, save_path, file_name, process_counter)
    crawler()
    return None


def main(options: Dict) -> None:
    # Metadata crawling
    options.num_of_processes = 49
    options.url = "https://api.themoviedb.org/3/discover/movie?include_adult=false&include_video=false&"
    options.file_name = "metadata"

    save_path = os.path.join(options.data_path, options.file_name)
    fixed_configurations = (options.headers, options.lang, options.url, save_path, options.file_name)

    # multiprocessor = Multiprocessor(options.start_year, options.end_year, fixed_configurations, options.num_of_processes)
    # print("Just spawn", len(multiprocessor.configurations), 'processes for the sake of balanced interval')
    # pp(multiprocessor.configurations)
    # multiprocessor(metadata_crawler_init, fixed_configurations)
    #
    # # Merge files & Filter en films
    # merge_file(data_path=options.data_path, file_name=options.file_name)
    # en_movie_filtering(os.path.join(save_path, f"{options.file_name}.json"))  # en: 261734 films (verified)



    # Movie detail crawling
    num_of_en_movies = count_en_movies(os.path.join(save_path, f"{options.file_name}.json"))
    options.num_of_processes = 50
    options.url = "https://api.themoviedb.org/3/movie/"
    options.file_name = "movie_detail"

    save_path = os.path.join(options.data_path, options.file_name)
    fixed_configurations = (options.headers, options.lang, options.url, save_path, options.file_name)

    multiprocessor = Multiprocessor(lower=0, upper=num_of_en_movies, fixed_configurations=fixed_configurations,
                                    processes=options.num_of_processes)
    print("Spawned", len(multiprocessor.configurations), "processes")
    # pp(multiprocessor.configurations)
    multiprocessor(mov ie_detail_crawler_init, fixed_configurations)
    return None


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--lang", type=str, default="language=en-US&")
    parser.add_argument("--headers", type=dict, default={"accept": "application/json",
                                                         "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJkMjFhODEzOTg5MWY0NDU0YmI3MmMwOTRkZjk4MjMxMSIsInN1YiI6IjY0YWUyMTE2M2UyZWM4MDBhZjdmOTI5NiIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.u85xU7i1cX_jR69x4OBq24kDtOIdvpK3FbYLffwBWSU"})
    parser.add_argument("--data_path", type=str, default=os.path.join(os.getcwd(), "data", "raw_data"))

    parser.add_argument("--num_of_processes", type=int)
    parser.add_argument("--url", type=str)
    parser.add_argument("--file_name", type=str)

    # Metadata only
    parser.add_argument("--start_year", type=int, default=1900)
    parser.add_argument("--end_year", type=int, default=2024)


    # For movie detail
    # Movie detail just needs id from metadata for scraping
    # parser.add_argument("--movie_detail_url", type=str, default="https://api.themoviedb.org/3/movie/")
    # parser.add_argument("--movie_detail_file_name", type=str, default="movie_detail")

    options: Namespace = parser.parse_args()

    main(options)
