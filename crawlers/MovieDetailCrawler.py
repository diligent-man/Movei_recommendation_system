import os
import sys
sys.path.append("utils")

import time
import json

import pandas as pd
import requests as rq

from tqdm import tqdm
from typing import List
from utils.json_decoder import json_decoder
from AbstractCrawler import TMDBCrawler

class MovieDetailCrawler(TMDBCrawler):
    def __init__(self, start: int, end: int,
                 headers: dict, lang: str, url: str,
                 save_path: str, file_name: str,
                 metadata_path: str, process_counter: int) -> None:
        super().__init__(start, end, headers, lang, url, save_path, file_name, process_counter)
        self._metadata_path = metadata_path

    # Support methods
    def __get_indices_df(self) -> pd.DataFrame:
        """
        Read metedata.json and select range of designated indexes
        return: dataframe["id"]
        """
        indices_df = None
        with open(file=self._metadata_path, mode="r", encoding="UTF-8", errors="ignore") as f:
            df = pd.DataFrame(next(json_decoder(f.read())))
            df = df.iloc[self._start: self._end]
            indices_df = df["id"]
        return indices_df


    # Main Methods
    def __call__(self) -> None:
        """
        Returns data format: {obj1},
                             {obj2},
                             {obj3},
                             ...
        """
        file_name = os.path.join(self._save_path, f"{self._process_counter}.json")
        indices_df = self.__get_indices_df()

        # start crawling
        for i in tqdm(indices_df.index,
                      position=self._process_counter,
                      desc=f"Process: {self._process_counter}, {self._start} to {self._end}",
                      colour='white'):
            url = self._url + str(indices_df[i]) + "?" + self._lang
            response = rq.get(url, headers=self._headers)
            json_object = json.dumps(response.json(), indent=4)

            # save to file
            with open(file=file_name, mode="a", encoding="UTF-8") as f:
                f.write(json_object + ",\n")

            time.sleep(.2)
        return None