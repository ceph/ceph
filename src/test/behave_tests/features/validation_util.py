
def str_to_list(string):
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


def assert_str_in_list(keyword_list, output_list):
    for keyword in keyword_list:
        assert keyword in output_list, f" Not found {keyword}"
