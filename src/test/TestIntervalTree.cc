// -*-#define dbg(lvl)\ mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include <cstdio>
#include <iostream>

#include "mds/IntervalTree.h"


#define ARRAY_SIZE(a)\
	sizeof(a)/sizeof(a[0])

using namespace std;

struct String {
  String(const string& s):str(s)
  {
  }

  bool operator <(const String& rhs) const
  {
    return str < rhs.str;
  }

  bool operator ==(const String& rhs) const
  {
    return str == rhs.str;
  }
 
  friend ostream& operator<<(ostream& out, const String& );

  string toString() const
  {
    return str;
  }

  string str;
};


struct LockHolder {
  LockHolder(char nm):
  name(nm)
  {
  }
  char name;

  bool operator <(const LockHolder& rhs) const
  {
    return name < rhs.name; 
  }
  bool operator ==(const LockHolder& rhs) const
  {
    return(name == rhs.name); 
  }
  string toString() const
  {
    return std::string(&name, 1);
  }

  friend ostream& operator<<(ostream& out, const LockHolder& );
 
};

struct Node {
  typedef int value_type;


  Node(value_type s, value_type e): left(s), right(e)
  {
  }


  std::string toString() const
  {
    char buff[50];
    memset(buff, 0, 50);
    snprintf(buff, 50, "left:%d, right:%d", left, right);

    return buff;
  }


  bool operator== (const Node& rhs) const
  {
    return((this->left == rhs.left) && (this->right == rhs.right));
  }

  bool operator< (const Node& rhs) const
  {
    return left <rhs.left;
  }

  friend ostream& operator<<(ostream& out, const Node& );
 

  value_type left;
  value_type right;
};

ostream& operator<< (ostream& out,  const LockHolder& t )
{
    out << t.toString();
    return out;
}


ostream& operator<< (ostream& out, const String& t )
{
    out << t.toString();
    return out;
}


ostream& operator<< (ostream& out, const Node& t )
{
    out << t.toString();
    return out;
}

template<typename T>
bool popNode(std::list<T>& result, const T& n)
{
  if(result.front() == n) {
    result.pop_front();
    return true;
  }
  return false;
}

template<typename T>
bool hasNode(std::list<T>& result, const T& n)
{
  typename std::list<T>::iterator itr = find(result.begin(), result.end(), n);
  return (itr != result.end());
}

template<typename T>
void printResultSet(std::list<T>& result)
{
#ifdef DEBUG
  typename std::list<T>::iterator i, iEnd = result.end();
  for(i = result.begin(); i != iEnd; ++i){
    std::cout << "result ::{" << (*i)<< "} " << std::endl; 
  }
#endif
}

template<typename T, typename U>
void printTree(IntervalTree<T,U> &tree)
{
#ifdef DEBUG
  std::cout  << "============= tree ============" << std::endl;
  tree.printTree(std::cout);
  std::cout << "============= ============" << std::endl;
#endif
}

void testLocks()
{
  // 1. Create new lock and acquire lock from 1-10
  IntervalTree<int, LockHolder> tree;
  LockHolder lockA('A');
  tree.addInterval(1, 10, lockA);

  printTree(tree);
  std::list<LockHolder> result;

  {
    result = tree.getIntervalSet(1,10);
    assert(result.size() == 1);

    printResultSet(result);
  }

  // Create new lock and acquire from 3-15
  LockHolder lockB('B');
  tree.addInterval(3, 15, lockB);

  {
    result = tree.getIntervalSet(1,15);
    assert(result.size() == 2);
  }

  // Release all  locks  from 3-10
  tree.removeInterval(3,10);

  // Query the whole range
  result = tree.getIntervalSet(1,15);

  assert(result.size() == 2 && (result.front() == 'A') );
  result.pop_front();
  assert(result.front() == 'B');


  // Query the range held by only A
  result = tree.getIntervalSet(1,5);
  assert(result.front().name == 'A');

  // Query the range held by none
  result = tree.getIntervalSet(4,9);
  printResultSet(result);
  assert(!result.size());

  // Again acquire lock B from 3-15
  tree.addInterval(3, 15, lockB);
  result = tree.getIntervalSet(1,15);
  assert(result.size() ==2);


  // Query the range held by only B
  result = tree.getIntervalSet(4,10);
  assert((result.size() ==1) && (result.front().name == 'B'));


  // Again acquire lock A from 3-10
  tree.addInterval(3, 10, lockA);
  result = tree.getIntervalSet(3,10);
  assert(result.size() ==2);

  // Release only  locks held by B from 3-10	
  tree.removeInterval(3,10, lockB);


  // Query interval just released by B
  result = tree.getIntervalSet(3,9);
  assert((result.size() ==1) && (result.front().name == 'A'));


  // Query interval now partially owned by B
  result = tree.getIntervalSet(3,12);
  assert(result.size() ==2);


  // Add one more lock between 4-8
  LockHolder lockC('C');
  tree.addInterval(4, 8, lockC);

  printTree(tree);

  // Query the interval shared by A,B,C
  result = tree.getIntervalSet(3,12);
  assert(result.size() ==3);


  // Query the intervals shared by A, C
  result = tree.getIntervalSet(3, 6);
  assert(result.size() == 2 && (result.front() == 'A') );
  result.pop_front();
  assert(result.front() == 'C');

  // Add one more lock between 2-4
  LockHolder lockD('D');
  tree.addInterval(2, 4, lockD);

  // Query the intervals owned by A.B,C,D
  result = tree.getIntervalSet(2, 11);
  assert(result.size () == 4);
  assert(popNode(result, lockA) && popNode(result, lockB) &&  popNode(result, lockC) && popNode(result, lockD));

}


void testBoundaries()
{
  // 1. Create new lock and acquire lock from 1-10
  IntervalTree<int, LockHolder> tree;
  LockHolder lockA('A');
  tree.addInterval(1, 10, lockA);

  // 2. Create lock B on the edge
  LockHolder lockB('B');
  tree.addInterval(10, 15, lockB);

  //3. Query the edge
  std::list<LockHolder> result = tree.getIntervalSet(10, 10);
  printResultSet(result);
  assert((result.size() == 1) && (result.front() == 'B'));
}

void testRemoveFromMiddle()
{
  // 1. Create new lock and acquire lock from 1-10
  IntervalTree<int, LockHolder> tree;
  LockHolder lockA('A');
  tree.addInterval(1, 10, lockA);

  // 2. Create lock B on the edge
  LockHolder lockB('B');
  tree.addInterval(10, 15, lockB);

  // 3. Remove all locks from 6-12
  tree.removeInterval(6, 12);

  //4.  Query the removed interval
  std::list<LockHolder> result = tree.getIntervalSet(6, 11);
  assert(result.size() == 0);

  //5. Query the interval locked by A
  result = tree.getIntervalSet(2, 5);
  assert((result.size() == 1) && (result.front() == 'A'));
}


void testIntervalQuery()
{
  int arr[][2] ={

    {1888,1971},
    {1874,1951},
    {1843,1907},
    {1779,1828},
    {1756,1791},
    {1585, 1672}

  };

  Node a(1888, 1971);
  Node b(1874, 1951);
  Node c(1843, 1907);
  Node d(1779, 1828);
  Node e(1756, 1791);
  Node f(1585, 1672);

  IntervalTree<int,Node> tree;
  for(size_t i=0; i < ARRAY_SIZE(arr); i++) {
    Node nd (arr[i][0], arr[i][1]);
    tree.addInterval( arr[i][0], arr[i][1], nd);
  }


  std::list<Node> result = tree.getIntervalSet(1843, 1874);
  assert(result.size() == 1 && (result.front() == c));

  result = tree.getIntervalSet(1873, 1974);
  assert(result.size() == 3);
  assert(popNode(result, c) && popNode(result,b) && popNode(result,a));


  result = tree.getIntervalSet(1910, 1910);
  assert(result.size() == 2);
  assert(popNode(result, b) &&  popNode(result,a));

  result = tree.getIntervalSet(1829, 1842);
  assert(result.size() == 0); 

  result = tree.getIntervalSet(1829, 1845);
  assert(result.size() == 1);
  assert(popNode(result, c)); 
}

void testRemoveInterval()
{
  int arr[][2] ={

    {1888,1971},
    {1874,1951},
    {1843,1907},
    {1779,1828},
    {1756,1791},
    {1585, 1672}

  };

  Node a(1888, 1971);
  Node b(1874, 1951);
  Node c(1843, 1907);
  Node d(1779, 1828);
  Node e(1756, 1791);
  Node f(1585, 1672);

  IntervalTree<int,Node> tree;
  for(size_t i=0; i < ARRAY_SIZE(arr); i++) {
    Node nd (arr[i][0], arr[i][1]);
    tree.addInterval( arr[i][0], arr[i][1], nd);
  }

  printTree(tree);
  std::list<Node> result; 

  {
    tree.removeInterval(1951, 1972, a); 
    result = tree.getIntervalSet(1960, 1970);
    assert(result.size() == 0);
  }

  {
    result = tree.getIntervalSet(1890, 1900);
    assert(result.size() == 3);
    assert(popNode(result, c) && popNode(result, b) && popNode(result, a));
  }

  {
    tree.removeInterval(1875, 1890, c); 
    result = tree.getIntervalSet(1877, 1889);
    assert(!hasNode(result, c));
  }

  {
    result = tree.getIntervalSet(1906, 1910);
    assert(result.size() == 3);
    assert(hasNode(result, a));
    assert(hasNode(result, b));
    assert(hasNode(result, c));
  }

}

void testStringIntervals()
{
  IntervalTree<char, String> tree;

  tree.addInterval('a', 'c', string("ac"));
  tree.addInterval('a', 'f', string("af"));
  tree.addInterval('d', 'k', string("dk"));
  tree.addInterval('d', 'l', string("dl"));
  tree.addInterval('d', 'o', string("do"));
  tree.addInterval('t', 'z', string("tz"));

  printTree(tree);
  std::list<String> result;

  {
    result = tree.getIntervalSet('b', 'g');
    assert(result.size() == 5);
    assert(!hasNode(result, String(string("tz"))));
  }

  {
    result = tree.getIntervalSet('k', 'z');
    assert(result.size() == 3);
    assert(hasNode(result, String(string("dl"))));
    assert(hasNode(result, String(string("do"))));
    assert(hasNode(result, String(string("tz"))));

    printResultSet(result);
  }

}

void testNames()
{  
  IntervalTree<char, string> tree;

  tree.addInterval('j', 'y', string("jojy"));
  tree.addInterval('b', 'n', string("brian"));
  tree.addInterval('e', 's', string("sage"));
  tree.addInterval('f', 'g', string("gregsf"));
  tree.addInterval('h', 'y', string("yehudah"));

  printTree(tree);

  std::list<string> result;
  {
    result = tree.getIntervalSet('b', 'y');
    assert(result.size() == 5);

    printResultSet(result);
  }
  
  { 
    result = tree.getIntervalSet('j', 'm');
    assert(!hasNode(result, string("gregsf")));

    printResultSet(result);
  }

  {
    tree.removeAll("jojy");

    result = tree.getIntervalSet('b', 'z');
    assert(!hasNode(result, string("jojy")));

    printResultSet(result);
  }
}

int main()
{ 
  testIntervalQuery();  
  testRemoveInterval(); 
  testLocks();
  testRemoveFromMiddle();
  testBoundaries();
  testStringIntervals();
  testNames();
}
