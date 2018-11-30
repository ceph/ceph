# -*- coding: utf-8 -*-

def differentiate(data1, data2):
    """
    # >>> times = [0, 2]
    # >>> values = [100, 101]
    # >>> differentiate(*zip(times, values))
    0.5
    """
    if float(data2[0] - data1[0]) == 0:
        return 0
    return (data2[1] - data1[1]) / float(data2[0] - data1[0])
