import json


class Util:
    def __init__(self) -> None:
        pass

    @staticmethod
    def pretty_print(json_input):
        return json.dumps(json_input, sort_keys=False, indent=4)
