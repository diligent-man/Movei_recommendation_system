import os
import time
import json
import requests as rq

from tqdm import tqdm
from typing import List


class MetadataCrawler:
    def __init__(self, start_index: int, end_index: int,
                 headers: dict, lang: str, url: str,
                 save_path: str, file_name: str, process_counter: int) -> None:
        self.__start_index = start_index
        self.__end_index = end_index
        self.__headers = headers
        self.__lang = lang
        self.__process_counter = process_counter

        self.__save_path = save_path
        self.__file_name = file_name
        self.__url = url

    # Support methods

    # Main Methods
    def Crawl(self) -> None:
        """
        Returns data format: {obj1},
                             {obj2},
                             {obj3},
                             ...
        """
        file_name = os.path.join(self.__save_path, f"{self.__process_counter}.json")

        # start crawling

        return None
