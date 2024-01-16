import os

import time
import json
import requests as rq

from tqdm import tqdm
from typing import List


class MetadataCrawler:
    def __init__(self, start_year: int, end_year: int,
                 headers: dict, lang: str, url: str,
                 save_path: str, file_name: str, process_counter: int) -> None:
        self.__start_year = start_year
        self.__end_year = end_year
        self.__headers = headers
        self.__lang = lang
        self.__process_counter = process_counter

        self.__save_path = save_path
        self.__file_name = file_name
        self.__url = url

    # Support methods
    def __GetTotalPages(self, year) -> int:
        # For crawling metadata only
        # Get total pages for each year and page should be range from 1 to 500
        url = self.__url + self.__lang + "year=" + str(year)
        total_pages = rq.get(url, headers=self.__headers).json()['total_pages']
        return min(total_pages, 500)

    # Main Methods
    def __call__(self) -> None:
        """
        Returns data format: {obj1},
                             {obj2},
                             {obj3},
                             ...
        """
        file_name = os.path.join(self.__save_path, f"{self.__process_counter}.json")

        # start crawling
        for year in tqdm(range(self.__start_year, self.__end_year),
                         position=self.__process_counter,
                         desc=f"Process: {self.__process_counter}, {self.__start_year} to {self.__end_year}",
                         colour='white'):
            total_pages = self.__GetTotalPages(year)

            # Crawl page by page -- 1 page comprises 20 results
            for page in range(1, total_pages + 1):
                url = self.__url + self.__lang + "year=" + str(year) + "&" + "page=" + str(page)
                response = rq.get(url, headers=self.__headers)
                time.sleep(.25)

                # Test "results" field is available or not
                try:
                    result: list = response.json()["results"]
                    json_list: List[str] = [json.dumps(json_object, indent=4) for json_object in result]

                    # save to file
                    if len(json_list) != 0:
                        with open(file=file_name, mode="a", encoding="UTF-8") as f:
                            for json_object in json_list:
                                f.write(json_object + ",\n")
                except Exception as e:
                    print(e)
        return None
