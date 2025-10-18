# from jsonfuzzer.parser.injector import Injector
# from jsonfuzzer.parser.path_finder import PathFinder

from .injector import Injector
from .path_finder import PathFinder

from typing import Any, Dict, List, Union


class Fuzzer:
    def __init__(self) -> None:
        self.INJECTOR = Injector()
        self.PATH_FINDER = PathFinder()

    def generate_structure_parameter_permutations_for_payload(
        self,
        structure: Union[Dict[str, Any], List[Any]],
        paramater_paths: List[List[Union[str, int]]],
        value_to_inject: Any,
    ) -> List[Union[Dict[str, Any], List[Any]]]:

        structure_fuzz_list = []

        for param_path in paramater_paths:
            structure_fuzz_list.append(
                self.INJECTOR.modify_attribute_in_structure_by_path(
                    structure=structure,
                    path=param_path,
                    value_to_inject=value_to_inject,
                )
            )

        return structure_fuzz_list

    def generate_structure_permutations_for_payload(
        self,
        structure: Union[Dict[str, Any], List[Any]],
        paramater_paths: List[List[Union[str, int]]],
        value_to_inject: Any,
    ) -> List[Union[Dict[str, Any], List[Any]]]:
        structure_fuzz_list = []

        for param_path in paramater_paths:
            structural_payloads = self.INJECTOR.generate_structure_payloads_by_path(
                path=param_path, structure=structure, value_to_inject=value_to_inject
            )

            structure_fuzz_list.extend(
                structure_payload
                for structure_payload in structural_payloads
                if structure_payload not in structure_fuzz_list
            )

        return structure_fuzz_list

    def generate_structure_missing_attribute_permutations(
        self,
        structure: Union[Dict[str, Any], List[Any]],
        paramater_paths: List[List[Union[str, int]]],
    ) -> List[Union[Dict[str, Any], List[Any]]]:
        structure_fuzz_list = []

        for param_path in paramater_paths:
            missing_attribute_payloads = self.INJECTOR.generate_missing_attribute_permutations_for_structure_by_path(
                path=param_path, structure=structure
            )
            structure_fuzz_list.extend(
                structure_payload
                for structure_payload in missing_attribute_payloads
                if structure_payload not in structure_fuzz_list
            )

        return structure_fuzz_list
