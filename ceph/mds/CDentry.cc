
#include "CDentry.h"
#include "CDir.h"


// CDentry

void CDentry::remove() {
  dir->remove_child(this);
}

CDentry::CDentry(const CDentry& m) {
  throw 1; //std::cerr << "copy cons called, implement me" << endl;
}

// =
const CDentry& CDentry::operator= (const CDentry& right) {
  throw 1;//std::cerr << "copy op called, implement me" << endl;
}

  // comparisons
  bool CDentry::operator== (const CDentry& right) const {
	return name == right.name;
  }
  bool CDentry::operator!= (const CDentry& right) const {
	return name == right.name;
  }
  bool CDentry::operator< (const CDentry& right) const {
	return name < right.name;
  }
  bool CDentry::operator> (const CDentry& right) const {
	return name > right.name;
  }
  bool CDentry::operator>= (const CDentry& right) const {
	return name >= right.name;
  }
  bool CDentry::operator<= (const CDentry& right) const {
	return name <= right.name;
  }
