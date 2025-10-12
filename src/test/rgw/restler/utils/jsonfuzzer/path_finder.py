from typing import Dict, Any, List, Union


class PathFinder:
    def __init__(self) -> None:
        pass

    def map_structure(
        self,
        structure: Union[List[Any], Dict[str, Any]],
        stack: List[Union[str, int]] = None,
        depth: int = 0,
    ) -> List[List[Union[str, int]]]:
        """
        Map paths to primitives in structure

        Recursively itterates over nested dictionary / list structure to map out the path to all
        the primitives in a structure. Handles aribtrary depths / complex type combinations

        :param structure: The complex dict / list based structure to map out
        :type structure: Union[List[Any], Dict[str, Any]]
        :param stack: List of paths taken to get to current location in structure, defaults to None
        :type stack: List[Union[str, int]], optional
        :param depth: current level of recursion, defaults to 0
        :type depth: int, optional
        :return: List of lists containing the path to each primitive in the structure
        :rtype: List[List[Union[str, int]]]
        """

        if isinstance(structure, dict):
            return self._parse_dict(input_dict=structure, stack=stack, depth=depth)

        if isinstance(structure, list):
            return self._parse_list(input_list=structure, stack=stack, depth=depth)

        return stack  # Hit the bottom, we should save this chain

    def _parse_list(
        self, input_list: List[Any], stack=None, depth=0
    ) -> List[List[Union[str, int]]]:
        """
        Parses structures of type list

        Recursively parses all indexes in the list structure to map out each path to a primitive

        :param input_list: The structure to parse and recursively drill into
        :type input_list: List[Any]
        :param stack: List of paths taken to get to current location in structure, defaults to None
        :type stack: _type_, optional
        :param depth: current level of recursion, defaults to 0
        :type depth: int, optional
        :return: List of lists containing the path to each primitive in the structure
        :rtype: List[List[Union[str, int]]]
        """
        if stack is None:
            stack = []

        completed = []

        for index, item in enumerate(input_list):
            # Reset the stack incase a key contains children of varying depths
            stack = stack[:depth]
            stack.append(index)

            result = self.map_structure(structure=item, stack=stack, depth=depth + 1)

            # If the object is primitive the parse map structure call will return a primitive
            # otherwise we should merge the lists
            if not isinstance(item, list) and not isinstance(item, dict):
                completed.append(result)
            else:
                completed.extend(result)

        return completed

    def _parse_dict(
        self,
        input_dict: Dict[str, Any],
        stack=None,
        depth=0,
    ) -> List[List[Union[str, int]]]:
        """
        Parses structures of type dictionary

        Recursively parses all indexes in the dict structure to map out each path to a primitive

        :param input_dict: The structure to parse and recursively drill into
        :type input_dict: Dict[str, Any]
        :param stack: List of paths taken to get to current location in structure, defaults to None
        :type stack: _type_, optional
        :param depth: current level of recursion, defaults to 0
        :type depth: int, optional
        :return: List of lists containing the path to each primitive in the structure
        :rtype: List[List[Union[str, int]]]
        """
        if stack is None:
            stack = []

        completed = []

        for key, value in input_dict.items():
            stack = stack[:depth]
            stack.append(key)

            result = self.map_structure(structure=value, stack=stack, depth=depth + 1)

            if not isinstance(value, list) and not isinstance(value, dict):
                completed.append(result)
            else:
                completed.extend(result)

        return completed
