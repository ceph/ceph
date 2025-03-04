from unittest.mock import MagicMock
import json
import pytest
from typing import Annotated

from mgr_module import CLICommand, CLI_ANNOTATIONS
from ceph_argparse import KB, MB

@pytest.fixture(scope="class", name="command_with_size_annotation_name")
def fixture_command_with_size_annotation_name():
    return "test annotated size command"


@pytest.fixture(scope="class", name="command_with_size_annotation")
def fixture_command_with_size_annotation(command_with_size_annotation_name):
    @CLICommand(command_with_size_annotation_name)
    def func(_, param: Annotated[int, CLI_ANNOTATIONS.MemorySize]): # noqa # pylint: disable=unused-variable
        return {'a': '1', 'param': param}
    yield func 
    del CLICommand.COMMANDS[command_with_size_annotation_name]
    assert command_with_size_annotation_name not in CLICommand.COMMANDS


class TestConvertAnnotatedType:
    def test_command_convert_annotated_parameter(self, command_with_size_annotation, command_with_size_annotation_name):
        result = CLICommand.COMMANDS[command_with_size_annotation_name].call(MagicMock(), {"param": f"{5 * 1024 ** 2}"})
        assert result['param'] == 5 * 1024 ** 2
        
        result = CLICommand.COMMANDS[command_with_size_annotation_name].call(MagicMock(), {"param": f"{5 * 1024}{KB}"})
        assert result['param'] == 5 * 1024 ** 2
        
        result = CLICommand.COMMANDS[command_with_size_annotation_name].call(MagicMock(), {"param": f"5{MB}"})
        assert result['param'] == 5 * 1024 ** 2
