from typing import Any, Dict, List

from ..exceptions import DashboardException


class ListPaginator:
    # pylint: disable=W0102
    def __init__(self, offset: int, limit: int, sort: str, search: str,
                 input_list: List[Any], default_sort: str,
                 searchable_params: List[str] = [], sortable_params: List[str] = []):
        self.offset = offset
        if limit < -1:
            raise DashboardException(msg=f'Wrong limit value {limit}', code=400)
        self.limit = limit
        self.sort = sort
        self.search = search
        self.input_list = input_list
        self.default_sort = default_sort
        self.searchable_params = searchable_params
        self.sortable_params = sortable_params
        self.count = len(self.input_list)

    def get_count(self):
        return self.count

    def find_value(self, item: Dict[str, Any], key: str):
        # dot separated keys to lookup nested values
        keys = key.split('.')
        value = item
        for nested_key in keys:
            if nested_key in value:
                value = value[nested_key]
            else:
                return ''
        return value

    def list(self):
        end = self.offset + self.limit
        # '-1' is a special number to refer to all items in list
        if self.limit == -1:
            end = len(self.input_list)

        if not self.sort:
            self.sort = self.default_sort

        desc = self.sort[0] == '-'
        sort_by = self.sort[1:]

        if sort_by not in self.sortable_params:
            sort_by = self.default_sort[1:]

        # trim down by search
        trimmed_list = []
        if self.search:
            for item in self.input_list:
                for searchable_param in self.searchable_params:
                    value = self.find_value(item, searchable_param)
                    if isinstance(value, (int, str)):
                        if self.search in str(value):
                            trimmed_list.append(item)

        else:
            trimmed_list = self.input_list

        def sort(item):
            return self.find_value(item, sort_by)

        sorted_list = sorted(trimmed_list, key=sort, reverse=desc)
        self.count = len(sorted_list)
        for item in sorted_list[self.offset:end]:
            yield item
