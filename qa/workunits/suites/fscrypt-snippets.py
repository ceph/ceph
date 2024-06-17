#! /usr/bin/env python3

import hashlib
import os
from time import sleep

def write_fill(fd, fill, size, offset):
  s = ''
  for i in range(0,size):
    s += fill

  os.lseek(fd, offset - int(size / 2), 0)
  os.write(fd, str.encode(s))

def test_overwrite_block_boundary():
  """Test writing data with small, half write on previous block and trailing on new block"""

  file = 'file.log'
  fd = os.open(file, os.O_RDWR|os.O_CREAT)

  s = write_fill(fd, 's', 5529, 6144)
  sleep(10)
  s = write_fill(fd, 't', 4033, 6144)

  os.close(fd)
  os.remove(file)

def test_huge_hole():
  """Test writing data with huge hole, half write on previous block and trailing on new block"""

  file = 'file.log'
  fd = os.open(file, os.O_RDWR|os.O_CREAT)

  s = write_fill(fd, 's', 4096, 107374182400)
  sleep(10)
  s = write_fill(fd, 't', 8, 16)

  os.close(fd)
  os.remove(file)

def test_med_hole_write_boundary():
  """Test writing data past many holes on offset 0 of block"""

  file = 'file.log'

  fd = os.open(file, os.O_RDWR|os.O_CREAT)

  #reproducing sys calls after ffsb bench has started
  fill = '\0'
  size = 3192
  offset = 60653568

  s = ''
  for i in range(0,size):
    s += fill

  os.lseek(fd, offset, 0)
  os.write(fd, str.encode(s))

  os.close(fd)
  os.remove(file)

def test_simple_rmw():
  """ Test simple rmw"""

  file = 'file.log'
  match_hash='08723317846e79780c8c9521b0f4bc49'

  fd = os.open(file, os.O_RDWR|os.O_CREAT)

  s = write_fill(fd, 's', 32, 16)
  s = write_fill(fd, 't', 8, 16)

  os.close(fd)

  fd = os.open(file, os.O_RDWR|os.O_CREAT)
  m = hashlib.md5()

  m.update(os.read(fd, 32))
  os.close(fd)

  if match_hash != m.hexdigest():
    raise

  os.remove(file)

def test_truncate_overwrite():
  """ Test copy smaller file -> larger file gets new file size"""

  file1 = 'file1.log'
  file2 = 'file2.log'
  expected_size = 1024

  fd = os.open(file1, os.O_WRONLY|os.O_CREAT)
  os.close(fd)
  fd = os.open(file2, os.O_WRONLY|os.O_CREAT)
  os.close(fd)

  os.truncate(file1, 1048576)
  os.truncate(file2, 1024)

  #simulate copy file2 -> file1
  fd = os.open(file1, os.O_WRONLY|os.O_TRUNC)
  fd2 = os.open(file2, os.O_RDONLY)
  os.copy_file_range(fd2, fd, 9223372035781033984)
  os.close(fd)
  os.close(fd2)

  if os.stat(file1).st_size != expected_size:
    raise

  os.remove(file1)
  os.remove(file2)

def test_truncate_path():
  """ Test overwrite/cp displays effective_size and not real size"""

  file = 'file1.log'
  expected_size = 68686

  #fstest create test1 0644;
  fd = os.open(file, os.O_WRONLY|os.O_CREAT)
  os.close(fd)
  #fstest truncate test1 68686;
  os.truncate(file, expected_size)

  #fstest stat test1 size
  if os.lstat(file).st_size != expected_size:
      raise
  #stat above command returns 69632 instead of truncated value.

  os.remove(file)

def test_lchown_symlink():
  """ Test lchown to ensure target is set"""

  file1 = 'file1.log'
  fd = os.open(file1, os.O_WRONLY|os.O_CREAT)
  os.close(fd)

  #fstest symlink file1 symlink1
  file2 = 'symlink'
  os.symlink(file1, file2)

  #fstest lchown symlink1 135 579
  os.lchown(file2, 135, 579)

  # ls -l
  #-rw-r--r--. 1 root root  0 Apr 22 18:11 file1
  #lrwxrwxrwx. 1  135  579 46 Apr 22 18:11 symlink1 -> ''$'\266\310''%'$'\005''W'$'\335''.'$'\355\211''kblD'$'\300''gq'$'\002\236\367''3'$'\255\201\001''Z6;'$'\221''&'$'\216\331\177''Q'
  if os.readlink(file2) != file1:
      raise

  os.remove(file1)
  os.remove(file2)

def main():
  test_overwrite_block_boundary()
  test_huge_hole()
  test_med_hole_write_boundary()
  test_simple_rmw()
  test_truncate_overwrite()
  test_truncate_path()
  test_lchown_symlink()

if __name__ == '__main__':
  main()
