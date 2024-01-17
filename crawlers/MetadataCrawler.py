import os
import sys
sys.path.append(os.path.join(os.getcwd(), "crawlers"))

import time
import json
import requests as rq

from tqdm import tqdm
from typing import List, Type
from AbstractCrawler import TMDBCrawler


class MetadataCrawler(TMDBCrawler):
    def __init__(self, start: int, end: int,
                 headers: dict, lang: str, url: str,
                 save_path: str, file_name: str, process_counter: int) -> None:
        super().__init__(start, end, headers, lang, url, save_path, file_name, process_counter)

    # Support methods
    def __GetTotalPages(self, year) -> int:
        # For crawling metadata only
        # Get total pages for each year and page should be range from 1 to 500
        url = self._url + self._lang + "year=" + str(year)
        total_pages = rq.get(url, headers=self._headers).json()['total_pages']
        return min(total_pages, 500)

    # Main Methods
    def __call__(self) -> None:
        """
        Returns data format: {obj1},
                             {obj2},
                             {obj3},
                             ...
        """
        file_name = os.path.join(self._save_path, f"{self._process_counter}.json")

        # start crawling
        for year in tqdm(range(self._start, self._end),
                         position=self._process_counter,
                         desc=f"Process: {self._process_counter}, {self._start} to {self._end}",
                         colour='white'):
            total_pages = self.__GetTotalPages(year)

            # Crawl page by page -- 1 page comprises 20 results
            for page in range(1, total_pages + 1):
                url = self._url + self._lang + "&year=" + str(year) + "&" + "page=" + str(page)
                response = rq.get(url, headers=self._headers)
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
