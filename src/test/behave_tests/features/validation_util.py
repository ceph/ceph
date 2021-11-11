from typing import List


def str_to_list(string: str) -> List[List[str]]:
    """
    Converts the string into list removing whitespaces
    """
    string = string.replace('\t', '\n')
    return [
        [
            key for key in line.split(' ')
            if key != ''
        ]
        for line in string.split('\n')
        if line != ''
    ]


def missing_tokens(expected: str, actual: str) -> List[List[str]]:
    # this line checks if, for each row of tokens in expected there exists
    # a row of tokens in actual that contains all the tokens. Any line of tokens
    # in expected that has no match in actual will be part of the return value
    return [s for s in str_to_list(expected) if not
            any(all([True if s2 in s3 else False for s2 in s]) for s3 in str_to_list(actual))]
