// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * diffsorted -- a utility to compute a line-by-line diff on two
 * sorted input files
 *
 * Copyright © 2019 Red Hat
 *
 * Author: J. Eric Ivancich
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.
 */


/*
 * SUMMARY
 *
 * The `diffsorted` utility does a line-by-line diff on two sorted text
 * files and indicating lines that are in one file but not the other
 * using diff-style notation (although line numbers are not indicated).
 *
 * USAGE
 *
 *     rgw-diff-sorted file1.txt file2.txt
 *
 * NOTES
 *
 * Each files should have its lines in sorted order and should have no
 * empty lines.
 *
 * A potential input file can be sorted using the `sort` utility provided
 * that LANG=C to insure byte lexical order. For example:
 *
 *     LANG=C sort unsorted.txt >sorted.txt
 *
 * or:
 *
 *     export LANG=C
 *     sort unsorted.txt >sorted.txt
 *
 * EXIT STATUS
 *
 *     0 : files same
 *     1 : files different
 *     2 : usage problem (e.g., wrong number of command-line arguments)
 *     3 : problem opening input file
 *     4 : bad file content (e.g., unsorted order or empty lines)
 */


#include <iostream>
#include <fstream>


struct FileOfLines {
  const char* filename;
  std::ifstream input;
  std::string this_line, prev_line;
  bool next_eof;
  bool is_eof;

  FileOfLines(const char* _filename) :
    filename(_filename),
    input(filename),
    next_eof(false),
    is_eof(false)
  { }

  void dump(const std::string& prefix) {
    do {
      std::cout << prefix << this_line << std::endl;
      advance();
    } while (!eof());
  }

  bool eof() const {
    return is_eof;
  }

  bool good() const {
    return input.good();
  }

  void advance() {
    if (next_eof) {
      is_eof = true;
      return;
    }

    prev_line = this_line;
    std::getline(input, this_line);
    if (this_line.empty()) {
      if (!input.eof()) {
	std::cerr << "Error: " << filename << " has an empty line." <<
	  std::endl;
	exit(4);
      }
      is_eof = true;
      return;
    } else if (input.eof()) {
      next_eof = true;
    }

    if (this_line < prev_line) {
      std::cerr << "Error: " << filename << " is not in sorted order; \"" <<
	this_line << "\" follows \"" << prev_line << "\"." << std::endl;
      exit(4);
    }
  }

  const std::string line() const {
    return this_line;
  }
};

int main(int argc, const char* argv[]) {
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " <file1> <file2>" << std::endl;
    exit(2);
  }

  FileOfLines input1(argv[1]);
  if (!input1.good()) {
    std::cerr << "Error opening " << argv[1] <<
      "." << std::endl;
    exit(3);
  }

  FileOfLines input2(argv[2]);
  if (!input2.good()) {
    std::cerr << "Error opening " << argv[2] <<
      "." << std::endl;
    exit(3);
  }

  bool files_same = true;

  input1.advance();
  input2.advance();

  while (!input1.eof() && !input2.eof()) {
    if (input1.line() == input2.line()) {
      input1.advance();
      input2.advance();
    } else if (input1.line() < input2.line()) {
      files_same = false;
      std::cout << "< " << input1.line() << std::endl;
      input1.advance();
    } else {
      files_same = false;
      std::cout << "> " << input2.line() << std::endl;
      input2.advance();
    }
  }

  if (!input1.eof()) {
    files_same = false;
    input1.dump("< ");
  } else if (!input2.eof()) {
    files_same = false;
    input2.dump("> ");
  }

  if (files_same) {
    exit(0);
  } else {
    exit(1);
  }
}
