// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2012 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include <vector>
#include <sstream>
#include <iomanip>
#include <string>
#include "include/assert.h"

/**
 * TextTable:
 * Manage tabular output of data.  Caller defines heading of each column
 * and alignment of heading and column data,
 * then inserts rows of data including tuples of
 * length (ncolumns) terminated by TextTable::endrow.  When all rows
 * are inserted, caller asks for output with ostream <<
 * which sizes/pads/dumps the table to ostream.
 *
 * Columns autosize to largest heading or datum.  One space is printed
 * between columns.
 */

class TextTable {

public:
  enum Align {LEFT = 1, CENTER, RIGHT};

private:
  struct TextTableColumn {
    std::string heading;
    int width;
    Align hd_align;
    Align col_align;

    TextTableColumn() {}
    TextTableColumn(std::string h, int w, Align ha, Align ca) :
		    heading(h), width(w), hd_align(ha), col_align(ca) { }
    ~TextTableColumn() {}
  };

  std::vector<TextTableColumn> col;	// column definitions
  unsigned int curcol, currow;		// col, row being inserted into
  unsigned int indent;			// indent width when rendering

protected:
  std::vector<std::vector<std::string> > row;	// row data array

public:
  TextTable(): curcol(0), currow(0), indent(0) {}
  ~TextTable() {}

  /**
   * Define a column in the table.
   *
   * @param heading Column heading string (or "")
   * @param hd_align Alignment for heading in column
   * @param col_align Data alignment
   *
   * @note alignment is of type TextTable::Align; values are
   * TextTable::LEFT, TextTable::CENTER, or TextTable::RIGHT
   *
   */
  void define_column(const std::string& heading, Align hd_align,
		     Align col_align);

  /**
   * Set indent for table.  Only affects table output.
   *
   * @param i Number of spaces to indent
   */
  void set_indent(int i) { indent = i; }

  /**
   * Add item to table, perhaps on new row.
   * table << val1 << val2 << TextTable::endrow;
   *
   * @param: value to output.
   *
   * @note: Numerics are output in decimal; strings are not truncated.
   * Output formatting choice is limited to alignment in define_column().
   *
   * @return TextTable& for chaining.
   */

  template<typename T> TextTable& operator<<(const T& item)
  {
    if (row.size() < currow + 1)
      row.resize(currow + 1);

    /**
     * col.size() is a good guess for how big row[currow] needs to be,
     * so just expand it out now
     */
    if (row[currow].size() < col.size()) {
      row[currow].resize(col.size());
    }

    // inserting more items than defined columns is a coding error
    assert(curcol + 1 <= col.size());

    // get rendered width of item alone
    std::ostringstream oss;
    oss << item;
    int width = oss.str().length();
    oss.seekp(0);

    // expand column width if necessary
    if (width > col[curcol].width) {
      col[curcol].width = width;
    }

    // now store the rendered item with its proper width
    row[currow][curcol] = oss.str();

    curcol++;
    return *this;
  }

  /**
   * Degenerate type/variable here is just to allow selection of the
   * following operator<< for "<< TextTable::endrow"
   */

  struct endrow_t {};
  static endrow_t endrow;

  /**
   * Implements TextTable::endrow
   */

  TextTable &operator<<(endrow_t)
  {
    curcol = 0;
    currow++;
    return *this;
  }

  /**
   * Render table to ostream (i.e. cout << table)
   */

  friend std::ostream &operator<<(std::ostream &out, const TextTable &t);

  /**
   * clear: Reset everything in a TextTable except column defs
   * resize cols to heading widths, clear indent
   */

  void clear();
};
