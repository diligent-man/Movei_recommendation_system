import os
import sys
sys.path.append(os.path.join(os.getcwd(), "crawlers"))

from abc import ABC, abstractmethod

class TMDBCrawler(ABC):
    # Fields
    _start: int
    _end: int

    _headers: dict
    _lang: str
    _url: str

    _save_path: str
    _file_name: str

    _process_counter: int


    def __init__(self, start: int, end: int,
                 headers: dict, lang: str, url: str,
                 save_path: str, file_name: str, process_counter: int) -> None:
        self._start = start
        self._end = end

        self._headers = headers
        self._lang = lang
        self._process_counter = process_counter

        self._save_path = save_path
        self._file_name = file_name
        self._url = url

    @abstractmethod
    def __call__(self) -> None:
        # Implement your code
        pass


