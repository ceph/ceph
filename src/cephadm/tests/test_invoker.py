# Tests for cephadm_invoker.py - secure wrapper for executing cephadm commands
#
import hashlib
import io
import os
import sys
import tempfile
from pathlib import Path
from unittest import mock
import pytest

import cephadm_invoker as invoker


class TestInvoker:
    """Tests for main function."""

    def test_run_command_valid(self, monkeypatch, tmp_path):
        """Test 'run' command with valid binary."""
        content = b'#!/usr/bin/env python3\nprint("test")\n'
        hash_value = hashlib.sha256(content).hexdigest()
        test_file = tmp_path / f'cephadm.{hash_value}'
        test_file.write_bytes(content)

        monkeypatch.setattr('sys.argv', ['cephadm_invoker.py', 'run', str(test_file), 'ls'])

        with mock.patch('os.execve') as mock_execve:
            with pytest.raises(SystemExit) as exc_info:
                invoker.main()
            assert exc_info.value.code == 0
            mock_execve.assert_called_once()

    def test_run_command_hash_mismatch(self, monkeypatch, tmp_path):
        """Test 'run' command with hash mismatch."""
        content = b'#!/usr/bin/env python3\nprint("test")\n'
        wrong_hash = 'wronghash123'
        test_file = tmp_path / f'cephadm.{wrong_hash}'
        test_file.write_bytes(content)
        
        monkeypatch.setattr('sys.argv', ['cephadm_invoker.py', 'run', str(test_file), 'ls'])
        
        with pytest.raises(SystemExit) as exc_info:
            invoker.main()
        assert exc_info.value.code == 2

    def test_run_command_nonexistent(self, monkeypatch, tmp_path):
        """Test 'run' command with nonexistent binary."""
        nonexistent = tmp_path / 'nonexistent'
        monkeypatch.setattr('sys.argv', ['cephadm_invoker.py', 'run', str(nonexistent), 'ls'])
        
        with pytest.raises(SystemExit) as exc_info:
            invoker.main()
        assert exc_info.value.code == 1

    def test_deploy_command_success(self, monkeypatch, tmp_path):
        """Test 'deploy_binary' command."""
        temp_file = tmp_path / 'temp_cephadm'
        temp_file.write_text('#!/usr/bin/env python3\nprint("test")')
        final_path = tmp_path / 'cephadm'

        monkeypatch.setattr('sys.argv', [
            'cephadm_invoker.py',
            'deploy_binary',
            str(temp_file),
            str(final_path)
        ])

        result = invoker.main()
        assert result == 0
        assert final_path.exists()

    def test_deploy_command_temp_not_exist(self, monkeypatch, tmp_path):
        """Test 'deploy_binary' with nonexistent temp file."""
        temp_file = tmp_path / 'nonexistent'
        final_path = tmp_path / 'cephadm'
        
        monkeypatch.setattr('sys.argv', [
            'cephadm_invoker.py',
            'deploy_binary',
            str(temp_file),
            str(final_path)
        ])
        
        result = invoker.main()
        assert result == 1

    def test_check_existence_exists(self, monkeypatch, tmp_path):
        """Test 'check_binary' command when file exists."""
        test_file = tmp_path / 'test_file'
        test_file.write_text('content')
        
        monkeypatch.setattr('sys.argv', [
            'cephadm_invoker.py', 
            'check_binary',
            str(test_file)
        ])
        
        result = invoker.main()
        assert result == 0

    def test_check_existence_not_exists(self, monkeypatch, tmp_path):
        """Test 'check_binary' command when file doesn't exist."""
        test_file = tmp_path / 'nonexistent'

        monkeypatch.setattr('sys.argv', [
            'cephadm_invoker.py',
            'check_binary',
            str(test_file)
        ])

        result = invoker.main()
        assert result == 2

    def test_invalid_command(self, monkeypatch):
        """Test invalid command."""
        monkeypatch.setattr('sys.argv', ['cephadm_invoker.py', 'invalid_command'])
        with pytest.raises(SystemExit) as exc_info:
            invoker.main()
        # argparse exits with code 2 for invalid command
        assert exc_info.value.code == 2
