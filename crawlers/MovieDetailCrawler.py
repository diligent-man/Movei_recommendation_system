import os
import time
import json
import requests as rq

from tqdm import tqdm
from typing import List
from AbstractCrawler import TMDBCrawler

class MovieDetailCrawler(TMDBCrawler):
    def __init__(self, start: int, end: int,
                 headers: dict, lang: str, url: str,
                 save_path: str, file_name: str, process_counter: int) -> None:
        super().__init__(start, end, headers, lang, url, save_path, file_name, process_counter)

    # Support methods

    # Main Methods
    def __call__(self) -> None:
        """
        Returns data format: {obj1},
                             {obj2},
                             {obj3},
                             ...
        """
        print(self._start)
        # start crawling
        return None
