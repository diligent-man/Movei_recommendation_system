import re
import json
import pandas as pd
from json import JSONDecoder, JSONDecodeError


NOT_WHITESPACE = re.compile(r'\S')


def json_decoder(document: str, pos=0, decoder=JSONDecoder()):
    """
    Acceptable format for document:
        a/ Format 1: Single json obj
            '''{obj}'''
        b/ Format 2: Multiple json objs
            '''{obj_1},
               {obj_2},
               {obj_3}
            '''
        c/ Format 2: List of Single or Multiple json objs
            '''
            [{obj_1},
               {obj_2},
               {obj_3}]
            '''
    """
    while True:
        match = NOT_WHITESPACE.search(document, pos)
        if not match:
            return
        pos = match.start()

        try:
            obj, pos = decoder.raw_decode(document, pos)
        except JSONDecodeError as e:
            print(e)
            # do something sensible if there's some error
            raise
        yield obj


def single_object_demo() -> None:
    json_str = """{
                   "ID": 1,
                   "Name": "Nguyen Van A"
                  }"""

    decoded_json = json_decoder(json_str)

    # Convert to df if you want
    df = pd.DataFrame(decoded_json)
    print(df)
    return None


def multiple_object_demo() -> None:
    # With multiple objects, we DO NOT need COMMA among objects
    json_str = """{
                   "ID": 1,
                   "Name": "Nguyen Van A"
                  }

                  {
                   "ID": 2,
                   "Name": "Nguyen Van B"
                  }

                  {
                   "ID": 3,
                   "Name": "Nguyen Van C"
                  }
                  """

    decoded_json = json_decoder(json_str)

    # Convert to df if you want
    df = pd.DataFrame(decoded_json)
    print(df)
    return None


def list_object_demo() -> None:
    json_str = """[{
                   "ID": 1,
                   "Name": "Nguyen Van A"
                  },

                  {
                   "ID": 2,
                   "Name": "Nguyen Van B"
                  },

                  {
                   "ID": 3,
                   "Name": "Nguyen Van C"
                  }]
                  """

    decoded_json = json_decoder(json_str)

    # Convert to df if you want
    df = pd.DataFrame(next(decoded_json))
    print(df)
    return None


def mixed_object_demo() -> None:
    json_str = """{
                   "ID": 1,
                   "Name": "Nguyen Van A"
                  }

                  {
                   "ID": 2,
                   "Name": "Nguyen Van B"
                  }

                  {
                   "ID": 3,
                   "Name": "Nguyen Van C"
                  }

                  [{
                   "ID": 1,
                   "Name": "Nguyen Van A"
                  },

                  {
                   "ID": 2,
                   "Name": "Nguyen Van B"
                  },

                  {
                   "ID": 3,
                   "Name": "Nguyen Van C"
                  }]
                  """
    decoded_json = json_decoder(json_str)

    for obj in decoded_json:
        print(obj)
    return None


def main() -> None:
    # single_object_demo()
    # multiple_object_demo()
    # list_object_demo()
    # mixed_object_demo()
    return None


if __name__ == '__main__':
    main()
