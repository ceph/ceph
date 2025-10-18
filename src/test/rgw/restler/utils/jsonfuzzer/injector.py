from typing import Dict, Any, List, Union
import copy


class Injector:
    def __init__(self) -> None:
        pass

    def modify_attribute_in_structure_by_path(
        self,
        structure: Union[Dict[str, Any], List[Any]],
        path: List[Union[str, int]],
        value_to_inject: Any,
    ) -> Union[Dict[str, Any], List[Any]]:
        """
        Modifies an attribute in a structure

        Uses a list of keys / indexes to walk through a structure and modify the target
        parameter to a specific value. Deep copies the input structure to not accidentally
        modify the source structure. Purpose built to handle both list and dictionary object
        types natively.

        :param structure: The complex dict / list based structure to map out
        :type structure: Union[Dict[str, Any], List[Any]]
        :param path: List of keys to get to a primitive in a structure
        :type path: List[Union[str, int]]
        :param value_to_inject: Value to inject into the target parameter, can be a
            primitive or a complex structure
        :type value_to_inject: Any
        :return: A structure with the target parameter modified to the injection value
        :rtype: Union[Dict[str, Any], List[Any]]
        """
        # Take a deep copy to avoid accidentally changing a shared reference
        target_dict = copy.deepcopy(structure)

        current = target_dict
        for index, k in enumerate(path):

            # We have not reached our destination yet, keep digging
            if index + 1 != len(path):
                current = current[k]
                continue

            current[k] = value_to_inject
            break

        return target_dict

    def remove_attribute_in_structure_by_path(
        self,
        structure: Union[Dict[str, Any], List[Any]],
        path: List[Union[str, int]],
    ) -> Union[Dict[str, Any], List[Any]]:
        """
        Removes an attribute in a structure

        Uses a list of keys / indexes to walk through a structure and removes the target
        parameter from the structure. Deep copies the input structure to not accidentally
        modify the source structure. Purpose built to handle both list and dictionary object
        types natively.

        :param structure: The complex dict / list based structure to map out
        :type structure: Union[Dict[str, Any], List[Any]]
        :param path: List of keys to get to a primitive in a structure
        :type path: List[Union[str, int]]
        :param value_to_inject: Value to inject into the target parameter, can be a
            primitive or a complex structure
        :type value_to_inject: Any
        :return: A structure with the target parameter modified to the injection value
        :rtype: Union[Dict[str, Any], List[Any]]
        """
        # Take a deep copy to avoid accidentally changing a shared reference
        target_dict = copy.deepcopy(structure)

        current = target_dict
        for index, k in enumerate(path):

            # We have not reached our destination yet, keep digging
            if index + 1 != len(path):
                current = current[k]
                continue

            del current[k]
            break

        return target_dict

    def generate_structure_payloads_by_path(
        self,
        structure: Union[Dict[str, Any], List[Any]],
        path: List[Union[str, int]],
        value_to_inject: Any,
    ) -> List[Any]:
        """
        Generates a list of structures that have an injected value at each hierarchical point

        To demonstrate this, the `list_top` parameter contains three child structures
        {
            "top_level": "top",
            "nested_top": {"middle_level": "test"},
            "list_top": [
                    {
                        "name": {
                            "type": "test"
                        }
                    }
                ],
        }

        The path to modify type would be ['list_top', 0, 'name', 'type'] we should therefore
        generate structural payloads for each hierarchical parent child relationship:

        1. ['list_top]
        2. ['list_top', 0]
        3. ['list_top', 0, 'name']

        Practically this looks like:

        [
            {
                "top_level": "top",
                "nested_top": {
                    "middle_level": "test"
                },
                "list_top": [
                    {
                        "name": ":weeeeee:"
                    }
                ]
            },
            {
                "top_level": "top",
                "nested_top": {
                    "middle_level": "test"
                },
                "list_top": [
                    ":weeeeee:"
                ]
            },
            {
                "top_level": "top",
                "nested_top": {
                    "middle_level": "test"
                },
                "list_top": ":weeeeee:"
            }
        ]

        :param structure: The complex dict / list based structure to map out
        :type structure: Union[Dict[str, Any], List[Any]]
        :param path: List of keys to get to a primitive in a structure
        :type path: List[Union[str, int]]
        :param value_to_inject: Value to inject into the target parameter, can be a
            primitive or a complex structure
        :type value_to_inject: Any
        :return: A list of structures that contain the injection value at each possible hierarchical point
        :rtype: List[Any]
        """

        # resulting_list = list(first_list)
        # resulting_list.extend(payload for payload in second_list if x not in resulting_list)
        return [
            self.modify_attribute_in_structure_by_path(
                structure=structure,
                path=path[:-index],  # Cut the last key off, don't fuzz the primitive
                value_to_inject=value_to_inject,
            )
            for index, _ in enumerate(path)
            if path[:-index]
            != list()  # Avoid redundant payloads when there is nothing to change
        ]

    def generate_missing_attribute_permutations_for_structure_by_path(
        self,
        structure: Union[Dict[str, Any], List[Any]],
        path: List[Union[str, int]],
    ) -> List[Any]:

        return [
            self.remove_attribute_in_structure_by_path(
                structure=structure, path=path[: index + 1]
            )
            for index in range(0, len(path), 1)
        ]

    def _nest_value_in_dict(self, input, inject_value):
        result = {}
        if isinstance(input, dict):
            for k, v in input.items():
                result[k] = self._nest_value_in_dict(v, inject_value)
        else:
            # We have hit the bottom, inject the value
            return inject_value

        return result
