// -*-#define dbg(lvl)\ mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 * 
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 *	author: Jojy George Varghese 
 */

#ifndef CEPH_INTERVALTREE_H
#define CEPH_INTERVALTREE_H

#include <string>
#include <exception>
#include <stack>
#include <list>
#include <vector>
#include <algorithm>
#include <iostream>
#include <string.h>
#include <assert.h>
#include <iostream>

#define DEBUG_LVL 1

#define dbg(lvl)\
      if(lvl <= DEBUG_LVL)\
	std::cout <<"[DEBUG][" << __FILE__ << ":" << __FUNCTION__ << ":" << __LINE__ << "] :: " ;\
      if(lvl <= DEBUG_LVL)\
	std::cout

#define derr\
      std::cout <<"[ERROR][" << __FILE__ << ":" << __FUNCTION__ << ":" << __LINE__ << "] :: " ;std::cout

#define dendl\
      std::endl

class IntervalTreeException:public std::exception {
public:
  IntervalTreeException(const std::string& msg):message(msg)
  {}

  ~IntervalTreeException() throw()
  {}

  const char * what() const throw()
  {
    return message.c_str();
  }

protected:
  std::string message;
};

class BadParameterException: public IntervalTreeException {
public:
  BadParameterException(const std::string& msg):IntervalTreeException(msg)
  {}
};

template<typename U, typename T>
class IntervalTree {
public:

  typedef  std::list<T> NodeList;
  typedef  U value_type;

  IntervalTree():
  root(NULL)
  {
  }

  ~IntervalTree()  
  {
    destroy();
  }

private:
  IntervalTree(const IntervalTree&);
  IntervalTree& operator=(const IntervalTree&);

  struct IntervalNode {
    explicit IntervalNode(const T& in): node(in)
    {
    }

    T node;
    value_type start;
    value_type end;

    bool operator<(const IntervalNode& rhs) const
    {
      return start < rhs.start;
    }

    bool operator==(const IntervalNode& rhs) const
    {
      return(start == rhs.start && end == rhs.end && node == rhs.node);
    }
  };

  struct IntervalNodeDataPred {
    explicit IntervalNodeDataPred(const T& t):data(t)
    {
    }
    const T& data;
    bool operator()(const IntervalNode& rhs)
    {
      return rhs.node == data;
    }
  };

  struct IntervalRange {
    value_type start;
    value_type end;
    std::list<IntervalNode> intervalNodeSet;

    bool operator<(const IntervalRange& rhs) const
    {
      return start < rhs.start;
    }

    bool operator==(const IntervalRange& rhs) const
    {
      return(start == rhs.start && end == rhs.end);
    }

    void printRange(std::ostream&) const;
  };

  struct IntervalRangePredicate {
    value_type start;
    value_type end;

    bool operator() (const IntervalRange& rhs)
    {
      return((start != rhs.start) || (end != rhs.end));               
    }
  };

  struct RemoveIntervalRangePredicate {
    bool operator() (const IntervalRange& rhs)
    {
      return(rhs.intervalNodeSet.size() == 0);               
    }
  };

  struct RemoveNodePredicate {
    explicit RemoveNodePredicate(const T&  t): data(t)
    {
    }

    const T& data;
    bool operator() (const IntervalNode& rhs) const
    {
      return( data == rhs.node);               
    }
  };

  struct TreeNode {
    typedef enum {
      LEFT,
      RIGHT
    }Child;

#ifdef DEBUG
    ~TreeNode()
    {
    }
#endif

    TreeNode* left;
    TreeNode* right;
    TreeNode* parent;
    unsigned short height;
    value_type value;
    unsigned int refCount;
    bool visited;

    std::list<IntervalRange> rangeList;
    std::pair<TreeNode*, TreeNode*> spanNodes;

    // operations
    int getBalance() const;
    void addNode(TreeNode* node, Child);
    void adjustHeight();
    void printNodes(std::ostream&) const;

  };

  TreeNode* root;
  typedef void (IntervalTree::*NodeOperation)(TreeNode*, void*);

  ///////
  // Methods
  //////

public:

  void addInterval(const value_type& start, const value_type& end, const T& in)
  {
    dbg(20) << "=========== adding new interval :{%s}===========" << in << dendl;

    if(start > end) {
      throw BadParameterException("invalid range - left value is greater than right value");
    }

    insert(start);
    insert(end);

    IntervalNode inode(in);
    inode.start = start;
    inode.end = end;

    insertNode(inode, start);
    insertNode(inode, end);

#ifdef DEBUG
    dbg(20) << "tree after insert node: " << dendl;
    printTree(std::cout);
#endif
  }

  typename IntervalTree::NodeList getIntervalSet(const value_type& left, const value_type& right) const
  {
    dbg(20) << "searching for interval  {" <<  left << "-" << right <<"}" <<dendl;

    if(left > right) {
      throw BadParameterException("invalid range - left value is greater than right value");
    }

    IntervalRange range;
    range.start = left;
    range.end = right;
    std::list<T> nodeSet;
    search(left, range, nodeSet);
    search(right, range, nodeSet);

    nodeSet.sort();
    nodeSet.unique();
    return nodeSet;
  }

  void removeInterval(const value_type& start, const value_type& end, const T& in)
  {
    if(root == NULL) {
      derr << "tree not initialized" << dendl;
      return;
    }

    if(start > end) {
      throw BadParameterException("invalid range - left value is greater than right value");
    }

    std::list<IntervalNode> dispList, splitNodes;
    IntervalRange range;
    range.start = start;
    range.end = end;
    searchAndRemove(start, range, in, dispList);
    searchAndRemove(end,range,  in, dispList);  

    typename std::list<IntervalNode>::iterator i,j, jEnd, iEnd = dispList.end();
    for(i = dispList.begin(); i  != iEnd; ++i) {
      dbg(20) << "relocating node " << (*i).node << dendl;
      splitIntervalNode((*i), range, splitNodes);
      jEnd = splitNodes.end();
      for(j = splitNodes.begin(); j != jEnd; ++j) {
	dbg(20) << "split node " << (*j).node << "{ " << (*j).start << "-" << (*j).end <<"}" << dendl;
	insert((*j).start);
	insert((*j).end);
	insertNodeList(splitNodes);
      }
    }
  }

  void removeInterval(const value_type& left , const value_type& right)
  {
    if(root == NULL) {
      derr << "tree not initialized" <<dendl;
      return;
    }

    if(left > right) {
      throw BadParameterException("Invalid range -left value is greater than right value\n");
    }

    std::list<IntervalNode> dispNodes, splitNodes;
    IntervalRange range;
    range.start = left;
    range.end = right;
    remove(left, range, dispNodes);
    remove(right, range, dispNodes);

    typename std::list<IntervalNode>::iterator i,j, jEnd, iEnd = dispNodes.end();
    for(i = dispNodes.begin(); i  != iEnd; ++i) {
      dbg(20) << "relocating node " << (*i).node << dendl;
      splitIntervalNode((*i), range, splitNodes);
      jEnd = splitNodes.end();
      for(j = splitNodes.begin(); j != jEnd; ++j) {
	dbg(20) << "split node " << (*j).node << "{ " << (*j).start << "-" << (*j).end <<"}" << dendl;
	insert((*j).start);
	insert((*j).end);
	insertNodeList(splitNodes);
      }
    }
  }

  void removeAll(const T& in)
  {
    if(root == NULL) {
      derr << "tree not initialized" <<dendl;
      return;
    }

    struct DeleteNodeArg {
      DeleteNodeArg(const T& n,std::list<value_type>& list ):node(n), dList(list)
      {
      }
      const T& node;
      std::list<value_type>& dList;
    };

    std::list<IntervalNode> intNodeList;
    std::list<value_type> delList;
    struct DeleteNodeArg arg(in, delList);

    if(!delList.size()) {
      dbg(20) << "no nodes gets deleted" << dendl;
    }

    doTreeWalkOperation(&IntervalTree::deleteIntervalNode, (void*)&arg, false);
  }

  void printTree(std::ostream& out)
  {
    dbg(1) << "---------------- tree ------------- :::" <<dendl;
    doTreeWalkOperation(&IntervalTree::printTreeNode, (void*)&out, true);
    out << std::endl;
  }

  ///////
  // tree operation helpers
  //////

private:

  void destroy()
  {
    doTreeWalkOperation(&IntervalTree::freeNode, NULL, false);
  }

  void doIterativeRebalance(TreeNode* node)
  {
    TreeNode* last = NULL;
    std::list<IntervalNode> dispList;

    while(node) {
      node->adjustHeight();
      dbg(20) << "rebalancing" << toString(node) << dendl ;
      node = rebalanceTree(node, dispList);
      dbg(20) << "after rotation.... current root :" << toString(node) << dendl;
      last = node;
      node = node->parent;
    }
    root = last;

    insertNodeList(dispList);
  }

  void insertNodeList(std::list<IntervalNode>& nodeList)
  {
    typename std::list<IntervalNode>::iterator i = nodeList.begin(); 

    while(i != nodeList.end()) {
      dbg(20) << "inserting displaced node" << (*i).node << dendl;
      insertNode(*i, (*i).start);
      insertNode(*i, (*i).end);
      ++i;
    }
  }

  void remove(const value_type& value , const IntervalRange& range,  std::list<IntervalNode>& dispList)
  {
    assert(root != NULL);
    dbg(20) << "removing ' " << value << " ' range :{"  << range.start << "-"  << range.end << "}" << dendl;

    TreeNode* current = root;
    std::pair<TreeNode*, TreeNode*> spanNodes;
    std::vector<value_type> remTreeNodes;  

    while(current) {
      dbg(20) << "current :"<< toString(current) << dendl;
      spanNodes = current->spanNodes;

      typename std::list<IntervalRange>::iterator i, iEnd = current->rangeList.end();
      for(i = current->rangeList.begin(); i != iEnd; ++i) {
	IntervalRange& currRange = *i;
	if(isInterSect(range.start, range.end, currRange.start, currRange.end)) {
	  dispList.splice(dispList.end(), (*i).intervalNodeSet);
	  currRange.intervalNodeSet.clear();
	}
      }

      RemoveIntervalRangePredicate pred;              
      dbg(20) << "removing displaced lists. current list size :" << current->rangeList.size() << dendl;
      current->rangeList.remove_if(pred);
      dbg(20) << "after removal... list size :" << current->rangeList.size() << dendl;

      if(value < current->value) {
	current = current->left;
      }
      else {
	current = current->right;
      }
    }
  }

  bool isRangeEdge(TreeNode* node, const value_type& value)
  {
#ifdef DEBUG
    dbg(20) << "checking range of " << toString(node) << dendl;
    node->printNodes(std::cout);
#endif
    std::list<IntervalRange>& intRange = node->rangeList;
    if(intRange.size() == 0) {
      return false;
    }

    typename std::list<IntervalRange>::iterator i, iEnd = intRange.end();
    for(i = intRange.begin(); i != iEnd; ++i) {
      if(((*i).start == value)|| ((*i).end == value)) {
	return true;
      }
    }

    return false;
  }

  void doTreeWalkOperation(NodeOperation op, void* data, bool isInOrder)
  {
    assert(root != NULL);

    TreeNode* current = root;
    std::stack<TreeNode*> nodeStack;

    while(true) {
      if(current != NULL) {
	if(isInOrder) {
	  (this->*op)(current, data);
	}
	nodeStack.push(current);
	current = current->left;
      }
      else {
	if(nodeStack.empty()) {
	  break;
	}
	else {
	  current = nodeStack.top();
	  nodeStack.pop();
	  if(!isInOrder) {
	    (this->*op)(current, data);
	  }
	  current = current->right;
	}
      }       
    }
  }

  bool isEligibleForCleanup(TreeNode* node)
  {
    dbg(20) << "checking eligiblity of node : " << toString(node) << "for cleanup" << dendl;
    if(node == NULL) {
      return false;
    }

    std::pair<TreeNode*, TreeNode*> span = node->spanNodes;

    if((isRangeEdge (node, node->value)) 
       ||(isRangeEdge(span.first, node->value))
       ||(isRangeEdge(span.second, node->value))) {
      return false;
    }

    return true;
  }

  bool searchAndRemove(const value_type& searchVal, const IntervalRange& range, const T& in, std::list<IntervalNode>& displaceNodes)
  { 
    dbg(20) << "search val : " << searchVal << ", interval :{" << range.start << "-" << range.end << "}," << "removing :" << in << dendl;

    TreeNode* current = root;       
    std::pair<TreeNode*, TreeNode*> spanNodes;
    while(current) {
      dbg(20) << "current :"<< toString(current) << dendl;
      spanNodes = current->spanNodes;

      typename std::list<IntervalRange>::iterator i, iEnd = current->rangeList.end();
      for(i = current->rangeList.begin(); i != iEnd; ++i) {
	IntervalRange& currRange = *i;
	if(isInterSect(currRange.start, currRange.end, range.start, range.end)) {
	  std::list<IntervalNode>& nodeList = (*i).intervalNodeSet;
	  IntervalNodeDataPred pred(in);
	  typename std::list<IntervalNode>::iterator j = std::find_if(nodeList.begin(), nodeList.end(), pred);

	  if(j != nodeList.end()) {
	    dbg(20) << "found data node" << dendl;
	    displaceNodes.push_back(*j);
	    nodeList.remove(*j);            
	  }
	}
      }

      if(searchVal >= current->value) {
	current = current->right;
      }
      else {
	current = current->left;
      }
    }

    return false;
  }

  void deleteIntervalNode(TreeNode* node, void* data)
  {
    struct DeleteNodeArg {
      const T& node;
      std::list<value_type>& dList;
    };

    DeleteNodeArg* arg = static_cast<DeleteNodeArg*>(data);
    dbg(20) << "deleting data node :" << arg->node << "in current " <<  toString(node) <<dendl;

    std::list<IntervalRange>& intRange = node->rangeList;
    typename std::list<IntervalRange>::iterator i, iEnd = intRange.end();

    for(i = intRange.begin(); i != iEnd; ++i) {
      std::list<IntervalNode>& nodeList = (*i).intervalNodeSet;

      RemoveNodePredicate remNodePred(arg->node);
      nodeList.remove_if(remNodePred);                    
    }

    RemoveIntervalRangePredicate remRangePred;
    intRange.remove_if(remRangePred);

    /*
    if(isEligibleForCleanup(node)) {
      dbg(20) << "node eligible for cleanup" <<dendl;
      arg->dList.push_back(node->value);
    }
    */
  }

  void splitIntervalNode(const IntervalNode& inode, const IntervalRange& range,  std::list<IntervalNode>& nodeList) 
  {
    nodeList.clear();

    if((inode.start < range.start) || (inode.end > range.end)) {
      dbg(20) << "splitting node " << inode.node << dendl;
      IntervalNode newNode(inode.node);

      if(inode.start < range.start) {
	newNode.start = inode.start;

	if(inode.end > range.end) {
	  // case 1: original node extends range boundary at both ends
	  dbg(20) << "case 1 for node :" << inode.node << " interval node :{" <<  inode.start <<  " - " <<inode.end << "}" <<dendl;
	  newNode.end = range.start;
	  nodeList.push_back(newNode);

	  newNode.start = range.end;
	  newNode.end = inode.end;
	  nodeList.push_back(newNode);

	}
	else {
	  // case 2: original node extends range boundary at the left
	  // 		but falls short on the right		    

	  dbg(20) << "case 2 for node :" << inode.node << " interval node :{" <<  inode.start <<  " - " <<inode.end << "}" <<dendl;
	  newNode.start = inode.start;
	  newNode.end = range.start;
	  nodeList.push_back(newNode);
	}
      }

      else if(inode.end > range.end) {
	// case 3: original node extends its right boundary to the right of
	//	range       				
	dbg(20) << "case 3 for node :" << inode.node << " interval node :{" <<  inode.start <<  " - " <<inode.end << "}" <<dendl;
	newNode.start = range.end;
	newNode.end = inode.end;
	nodeList.push_back(newNode);
      }
    }
  }

  void linkParent(TreeNode* node, TreeNode* child) 
  {
    dbg(20) << "linking parent of " << toString(node) << "   to "<<  toString(child) << dendl;

    if(node->parent) {
      if(node->parent->left == node) {
	dbg(20) << "linking to parent " <<  toString(node->parent) << " 's left" <<dendl;
	node->parent->left = child;     
      }
      else if(node->parent->right == node) {
	dbg(20) << "linking to parent " <<  toString(node->parent) << " 's right" <<dendl;
	node->parent->right = child;
      }
    }
  }

  void adjustSpan(TreeNode* node) 
  {
    assert(node);
    node->spanNodes = getSpan(node);
  }

  void freeNode(TreeNode* node, void* data) 
  {
    assert(node);

    if(node->parent != NULL) {
      if(node->parent->left == node) {
	node->parent->left = NULL;
      }
      if(node->parent->right == node) {
	node->parent->right = NULL;
      }
    }

    delete node;
  }


  IntervalTree::TreeNode* deleteNode(IntervalTree::TreeNode* node, std::list<IntervalNode>& dispList) 
  {
    assert(node);

    dbg(20) << "deleting node " << toString(node) <<dendl;
#ifdef DEBUG
    dbg(20) << "disp nodes before delete :" << dendl;
    typename std::list<IntervalNode>::iterator j;
    for(j = dispList.begin() ; j != dispList.end(); ++j) {
      dbg(20) << "node :" <<  (*j).node << dendl;
    }
#endif
    TreeNode* parent;
    if(node->rangeList.size()) {
      typename std::list<IntervalRange>::iterator i ,iEnd = node->rangeList.end();
      for(i = node->rangeList.begin(); i != iEnd; ++i) {
	dispList.splice(dispList.end(), (*i).intervalNodeSet);      
      }
      node->rangeList.clear();
    }

    //
    // case 1. if no left and right child for the node getting deleted,
    // 	
    if((node->left == NULL) && (node->right == NULL)) {
      dbg(20) <<"left and right child are absent" << dendl;
      parent = node->parent;
      linkParent(node, NULL);
      TreeNode* prevNode = node->spanNodes.first;
      TreeNode* nextNode = node->spanNodes.second;
      adjustSpan(prevNode);
      adjustSpan(nextNode);
      freeNode(node);         
    }

    //
    // case 2. if has left/right child only
    //		 
    else if((node->left == NULL) || (node->right == NULL)) {
      dbg(20) << "left or right child is absent" << dendl;
      parent = node->parent;
      if(node->left != NULL) {
	dbg(20) << "left child present" << dendl;
	TreeNode* prevNode = node->spanNodes.first;
	if(prevNode->rangeList.size()) {
	  typename std::list<IntervalRange>::iterator i ,iEnd = prevNode->rangeList.end();
	  for(i = prevNode->rangeList.begin(); i != iEnd; ++i) {
	    dispList.splice(dispList.end(), (*i).intervalNodeSet);
	  }
	  prevNode->rangeList.clear();
	}
	linkParent(node, node->left);
	node->left->parent = node->parent;
	adjustSpan(prevNode);
      }
      if(node->right != NULL) {
	dbg(20) << "right child present" << dendl;
	TreeNode* nextNode = node->spanNodes.second;
	if(nextNode->rangeList.size()) {
	  typename std::list<IntervalRange>::iterator i ,iEnd = nextNode->rangeList.end();
	  for(i = nextNode->rangeList.begin(); i != iEnd; ++i) {
	    dispList.splice(dispList.end(), (*i).intervalNodeSet);
	  }
	  nextNode->rangeList.clear();
	}
	linkParent(node, node->right);
	node->right->parent = node->parent;
	adjustSpan(nextNode);
      }

      delete node;
      node = NULL;
    }

    //
    // case 3. if d has left AND right nodes
    //
    else {
      dbg(20) << "both left and right child present" << dendl;
      TreeNode* nextNode = node->spanNodes.second, *prevNode = node->spanNodes.first;
      parent = nextNode->parent;

      if(prevNode->rangeList.size()) {
	typename std::list<IntervalRange>::iterator i ,iEnd = prevNode->rangeList.end();
	for(i = prevNode->rangeList.begin(); i != iEnd; ++i) {
	  dispList.splice(dispList.end(), (*i).intervalNodeSet);
	}
	prevNode->rangeList.clear();
      }

      if(nextNode->rangeList.size()) {
	typename std::list<IntervalRange>::iterator i ,iEnd = nextNode->rangeList.end();
	for(i = nextNode->rangeList.begin(); i != iEnd; ++i) {
	  dispList.splice(dispList.end(), (*i).intervalNodeSet);
	}
	nextNode->rangeList.clear();
      }

      dbg(20) << "next node :" << toString(nextNode) << dendl;
      node->value = nextNode->value;

      linkParent(nextNode, nextNode->right);
      adjustSpan(node);
      adjustSpan(prevNode);
      if(nextNode->spanNodes.second) {
	adjustSpan(nextNode->spanNodes.second);
      }

      delete nextNode;       
      nextNode = NULL;
    }  

    dispList.sort();
    dispList.unique();

#ifdef DEBUG
    dbg(20) << "disp nodes after splice:" << dendl;
    typename std::list<IntervalNode>::iterator i;
    for(i = dispList.begin() ; i != dispList.end(); ++i) {
      dbg(20) << "node :" <<  (*i).node << dendl;
    }
#endif
    return parent;
  }


  void search(const value_type& value, const IntervalTree::IntervalRange& searchRange,  typename IntervalTree::NodeList& nodeSet) const
  {
    dbg(20) << "searching for value :" << value <<dendl;

    TreeNode* current = root;
    while(current) {
      if(current->rangeList.size()) {
	typename std::list<IntervalRange>::iterator i ,iEnd = current->rangeList.end();
	for(i = current->rangeList.begin(); i != iEnd; ++i) {
	  IntervalRange& range = (*i);
	  dbg(20) << "current node : " <<toString(current) << dendl;
#ifdef DEBUG
	  range.printRange(std::cout);
#endif
	  if(isInterSect(range.start, range.end, searchRange.start, searchRange.end)) {
	    dbg(20) << "range intersects " << dendl;
	    copyItems(searchRange, range.intervalNodeSet, nodeSet);
#ifdef DEBUG
	    dbg(20) << "nodes copies :" <<dendl ;
	    typename std::list<T>::iterator i;
	    for(i = nodeSet.begin() ; i != nodeSet.end(); ++i) {
	      dbg(20) << "node :" << (*i) <<dendl;
	    }
#endif
	  }

	}
      }

      if(current->value == value && (current->right != NULL)) {
	current = current->right;
      }
      else if((current->left != NULL) && (current->value >= value)) {
	current = current->left;
      }
      else if((current->right != NULL) && (current->value <= value)) {
	current = current->right;
      }
      else
	break;        

    }
  }

  void insertNodeInRange(TreeNode* current, const IntervalNode& in, IntervalRange& spanRange)
  {
    typename std::list<IntervalRange>::iterator i;
    typename std::list<IntervalRange>::iterator iEnd;
    dbg(20) << "spanRange :{" << spanRange.start << "-" <<   spanRange.end <<"}" << dendl;
    i = std::find(current->rangeList.begin(), current->rangeList.end(), spanRange);
#ifdef DEBUG
    dbg(20) << "current nodes :" <<dendl;
    current->printNodes(std::cout);
#endif
    if(i == current->rangeList.end()) {
      dbg(20) << " could not find the range .. will be adding now" << dendl;
      spanRange.intervalNodeSet.push_back(in);
      current->rangeList.push_back(spanRange);                        
    }
    else {
      dbg(20) << " range already in the list" << dendl;

      dbg(20) << "adding node " << in.node << "to the range list" << dendl;
      (*i).intervalNodeSet.push_back(in);
      (*i).intervalNodeSet.sort();
      (*i).intervalNodeSet.unique();

    }

    current->rangeList.sort();
    current->rangeList.unique();

#ifdef DEBUG
    dbg(20) << "nodes after :" <<dendl;
    current->printNodes(std::cout);
#endif
  }


  void insertNode(const IntervalNode& in, const value_type& value)
  {
    assert(root);

    TreeNode* current = root, *prevNode, *nextNode, *spanPrev, *spanNext;       
    while(current) {
      dbg(20) << "inserting node :" << in.node << " , value : " <<value << " ,  at current :" << toString(current)<< dendl;

      std::pair<TreeNode* ,TreeNode*> span = getSpan(current);
      prevNode = prev(current);           
      prevNode = prevNode == NULL ? current : prevNode;
      dbg(25) << "prev node :" << toString(prevNode) << dendl;

      nextNode = next(current);
      nextNode = nextNode == NULL ? current : nextNode;
      dbg(25) << "next node :" << toString(nextNode) << dendl;

      dbg(25) << "span for node " << toString(current) <<" : {" <<  toString(span.first) << " - " << toString(span.second) << "}" <<
	"prev node :" << toString(prevNode) << " , next node :" << toString(nextNode)  <<   dendl;

      current->spanNodes = span;
      IntervalRange spanRange;

      if(span.first != NULL && span.second != NULL) {
	spanPrev = prev(span.first);
	spanNext = next(span.second);
	dbg(25) << "span : {" << spanPrev->value << " - " << spanNext->value << "}" << dendl;
	if(spanPrev->value >= in.start && spanNext->value <= in.end) {
	  dbg(25) << "inserting over the full span " << dendl;

	  spanRange.start = spanPrev->value;
	  spanRange.end = spanNext->value;
	  insertNodeInRange(current, in, spanRange);
	  return;
	}

	if(current->value <= in.start && nextNode->value >= in.end) {
	  spanRange.start = current->value;
	  spanRange.end = nextNode->value;
	  insertNodeInRange(current, in, spanRange);
	  //return;
	}
      }
      else {
	span.first = (span.first == NULL) ? prevNode : prev(span.first);
	span.second = (span.second == NULL) ? nextNode : next(span.second);
	dbg(20) << "span :{" << span.first->value << " - " << span.second->value << "}" << dendl;
	if(span.first->value >= in.start && span.second->value <= in.end) {
	  spanRange.start = span.first->value;
	  spanRange.end = span.second->value;
	  insertNodeInRange(current, in, spanRange);
	  return;
	}
	if(span.first->value >= in.start && current->value <= in.end) {
	  spanRange.start = span.first->value;
	  spanRange.end = current->value;
	  insertNodeInRange(current, in, spanRange);
	  //return;
	}
	if(current->value >= in.start && span.second->value <= in.end) {
	  spanRange.start = current->value;
	  spanRange.end = span.second->value;
	  insertNodeInRange(current, in, spanRange);
	  //return;
	}
      }

      if((current->left != NULL) && ((current->value > value) || (span.first->value > in.start))) {
	current = current->left;
      }
      else if((current->right != NULL) && ( (current->value < value) || (span.second->value < in.end))) {
	current = current->right;
      }
      else
	break;          
    }
  }


  void insert(const value_type& in)
  {
    TreeNode* newNode = createTreeNode(in);
    dbg(20) << "adding new tree node :" << toString(newNode) <<dendl;

    if(NULL == root) {
      root = newNode;
      return;
    }

    TreeNode* current = root;       
    while(current) {
      dbg(20) << "current node :" << toString(current) <<dendl;
      if(current->value == in) {
	dbg(20) << "a node already exists with same value.. abandoning!!\n"  <<dendl;
	current->refCount++;
	break;
      }
      //TODO:  replace with comparator
      if(in <= current->value) {
	if(current->left == NULL) {
	  dbg(20) <<  "inserting new node " << toString(newNode)  << " to LEFT"  << dendl;
	  current->addNode(newNode, TreeNode::LEFT);              
	  break;
	}
	current = current->left;
      }
      else {
	if(NULL == current->right) {
	  dbg(20) <<  "inserting new node "  << toString(newNode)  << " to RIGHT"  << dendl;
	  current->addNode(newNode, TreeNode::RIGHT);                 
	  break;
	}
	current = current->right;
      }
    }       

    doIterativeRebalance(current);

#ifdef DEBUG
    dbg(20) << "@@ tree after insert " << dendl;
    printTree(std::cout);
#endif
  }


  bool isInterSect(const value_type& ax1, const value_type& ax2, const value_type& bx1, const value_type& bx2) const
  {
    dbg(25) << "ax1 :" << ax1 << ", ax2:" << ax2 << ", bx1 :" << bx1 << ", bx2 :" << bx2 << dendl; 
    if(
      (ax1 >= bx1 && ax1 < bx2)
      || (ax2 > bx1 && ax2 <= bx2)
      || (ax1 <= bx1 && ax2 > bx2)       
      ) {
      dbg(25) << "intersects!! " << dendl;
      return true;
    }

    return false;
  }


  void copyItems(const IntervalRange& range, typename std::list<IntervalNode>& src, typename std::list<T>& dest) const
  {
    typename std::list<IntervalNode>::iterator i ;
    typename std::list<IntervalNode>::iterator iEnd= src.end();

    for(i = src.begin(); i != iEnd; ++i) {
      if(isInterSect(range.start, range.end, (*i).start, (*i).end)) {
	dest.push_back((*i).node);
      }
    }
    dest.sort();
    dest.unique();
  }


  IntervalTree::TreeNode* createTreeNode(value_type in)
  {
    TreeNode* node = new TreeNode();
    node->left = NULL;
    node->right = NULL;
    node->parent = NULL;
    node->value = in;
    node->height = 0;
    node->refCount = 0;
    node->visited = false;

    return node;
  }


  void printTreeNode(IntervalTree::TreeNode* node, void* data)
  {
    if(node == NULL) {
      return;
    }

    std::ostream* out = static_cast<std::ostream*>(data);
    *out << std::endl ;
    *out <<  node << "--- {value :"  << node->value << "} {height : " 
      << node->height << "}  L : "<< (node->left? toString(node->left):"{NULL}") 
      << "  R: " << (node->right? toString(node->right) : "{NULL}");

    if(node->spanNodes.first && node->spanNodes.second) {
      *out << std::endl << "span : {" << node->spanNodes.first->value << ", " << 
	node->spanNodes.second->value << "} " << std::endl;
    }
    *out << std::endl;
    *out << "node lists :" << std::endl;

    typename std::list<IntervalRange>::iterator i;
    typename std::list<IntervalRange>::iterator iEnd = node->rangeList.end();

    for(i = node->rangeList.begin(); i != iEnd; ++i) {
      (*i).printRange(*out);
    }
  }

   std::string toString(const TreeNode* nd) const
  {
    if(NULL == nd) {
      return "{NULL}";
    }

    char buff[100];
    memset(buff, 0, 100);
    snprintf(buff, 100, "{addr :%p, value :%d, height :%u, left :{%p:%d}, right:{%p:%d}}", 
      nd, nd->value, nd->height, nd->left? nd->left:0, nd->left? nd->left->value: -999, 
      nd->right? nd->right:0, nd->right? nd->right->value: -999 );

    return buff;
  }

  TreeNode* doLeftRotation(TreeNode* n)
  {
    if(n == NULL) {
      derr << "cant left rotate a null node" << dendl;
      return n;
    }

    if(n->right == NULL) {
      dbg(20) << "no right child. cant do left rotation" <<dendl;
      return n;
    }

    dbg(20) << "left rotating " << toString(n->right) << " on pivot " <<  toString(n) <<dendl;
    TreeNode* root = n->right;
    TreeNode* left = root->left;
    TreeNode* save = n;

    root->left = save;
    save->right = left;
    if(left) {
      left->parent = save;
    }
    root->parent = save->parent;
    save->parent = root;

    save->adjustHeight();
    root->adjustHeight();

    dbg(20) << "returning root :" << toString(root) <<dendl;
    return root;
  }

  TreeNode* doRightRotation(TreeNode* n)
  {
    if(n == NULL) {
      dbg(20) << "cant left rotate a null node" <<dendl;
      return n;
    }

    if(n->left == NULL) {
      dbg(20) << "no left child. cant do right rotation" <<dendl;
      return n;
    }

    dbg(20) << "right rotating " <<  toString(n->left) << "on pivot :" << toString(n) << dendl;

    TreeNode* root = n->left;
    TreeNode* right = root->right;
    TreeNode* save = n;

    root->right = save;
    save->left = right; 
    if(right) {
      right->parent = save;
    }
    root->parent = save->parent;
    save->parent = root;

    save->adjustHeight();
    root->adjustHeight();

    dbg(20) << "after rotate root->right :" <<  toString(root->right) <<dendl;
    dbg(20) << "returning root : " <<  toString(root) <<dendl;
    return root;
  }


  void adjustSpanAndDisplace(TreeNode* current,  std::list<IntervalNode>& dispList)
  {
    TreeNode  *prevNode, *nextNode, *spanPrev, *spanNext;
    std::pair<TreeNode* ,TreeNode*> span = getSpan(current);
    prevNode = prev(current);           
    prevNode = prevNode == NULL ? current : prevNode;
    dbg(25) << "prev node :" << toString(prevNode) << dendl;

    nextNode = next(current);
    nextNode = nextNode == NULL ? current : nextNode;
    dbg(25) << "next node :" << toString(nextNode) << dendl;

    dbg(25) << "span for node " << toString(current) <<" : {" <<  toString(span.first) << " - " << toString(span.second) << "}" <<
      "prev node :" << toString(prevNode) << " , next node :" << toString(nextNode)  <<   dendl;

    current->spanNodes = span;

    typename std::list<IntervalRange>::iterator i;
    typename std::list<IntervalRange>::iterator iEnd;
    if(current->rangeList.size()) {
      iEnd = current->rangeList.end();
      for(i = current->rangeList.begin(); i != iEnd; ++i) {
	IntervalRange& range = *i;
	dbg(20) << "range :{" << range.start << "-" << range.end << "}" << dendl;
	bool isSpliceRequired = false;

	if(span.first != NULL  && span.second != NULL) {
	  spanPrev = prev(span.first);
	  spanNext = next(span.second);
	  if((range.start != spanPrev->value && range.start != current->value) || (range.end !=spanNext->value && range.end != nextNode->value)) {
	    dbg(20) << "spanPrev :' " << spanPrev->value << " ' , spanNext :' " << 
	      spanNext->value << " ', nextNode :' " << nextNode->value << " ' " << dendl ;
	    isSpliceRequired = true;
	  }
	}
	else {
	  if(span.first != NULL && span.second == NULL) {
	    spanPrev = prev(span.first);
	    if(range.start != spanPrev->value && range.start != current->value) {
	      isSpliceRequired = true;
	    }
	    if(range.end != nextNode->value && range.end != current->value) {
	      isSpliceRequired = true;
	    }
	  }
	  else if(span.first == NULL && span.second != NULL) {
	    spanNext = next(span.second);
	    if(range.start != current->value && range.start != prevNode->value) {
	      isSpliceRequired = true;
	    }
	    if(range.end != current->value && range.end !=spanNext->value) {
	      isSpliceRequired = true;
	    }
	  }
	  else {
	    if(range.start != prevNode->value && range.start !=current->value) {
	      isSpliceRequired = true;
	    }
	    if(range.end != current->value && range.end != nextNode->value) {
	      isSpliceRequired = true;
	    }
	  }
	}
	if(isSpliceRequired) {
	  dbg(20) << "splicing range :{" << range.start << " - " << range.end <<"}" <<dendl;
#ifdef DEBUG
	  range.printRange(std::cout);
#endif
	  dispList.splice(dispList.end(), range.intervalNodeSet);
	  dbg(20) << "after splicing setsize:" << range.intervalNodeSet.size() <<dendl;
	}
      }
      dispList.sort();
      dispList.unique();

      RemoveIntervalRangePredicate pred;              
      dbg(25) << "removing displaced lists. current list size :" << current->rangeList.size() << dendl;
      current->rangeList.remove_if(pred);
      dbg(25) << "range list size after cleanup:" <<  current->rangeList.size() << dendl;
    }
  }


  TreeNode* rebalanceTree(TreeNode* p,  std::list<IntervalNode>& dispList)
  {
    assert(p != NULL);

    dbg(20) << "rebalancing  :" << toString(p) <<dendl;
    TreeNode* root = p, *tmp = p; bool isLeft = false;
    if(root->parent != NULL) {
      isLeft = (root->parent->left == root);
    }

    if(p->getBalance() <= -2) {
      dbg(20) << "right side heavy" <<dendl;
      if((p->right != NULL) && (p->right->getBalance() <= -1)) {
	dbg(20) << "right child is right heavy" << dendl;
	tmp = doLeftRotation(root->right);              
	p->right = tmp;
	adjustSpanAndDisplace(root, dispList);
	root = doLeftRotation(root);

      }
      else if((p->right != NULL) && (p->right ->getBalance() >= 1)) {
	dbg(20) << "right child is left heavy" <<dendl;
	tmp = doRightRotation(root->right);
	p->right = tmp;
	adjustSpanAndDisplace(root, dispList);
	root = doLeftRotation(root);
      }

    }
    else if(p->getBalance() >= 2) {
      dbg(20) << "left side heavy" << dendl;
      if((p->left != NULL) && (p->left->getBalance() >= 1)) {
	dbg(20) << "left child is left heavy" <<dendl;
	tmp = doRightRotation(root->left);
	p->left = tmp;
	adjustSpanAndDisplace(root, dispList);
	root = doRightRotation(root);
      }
      else if((p->left != NULL) && (p->left ->getBalance() <= -1)) {
	dbg(20) << "left child is right heavy" <<dendl;
	tmp = doLeftRotation(root->left);
	p->left = tmp;
	adjustSpanAndDisplace(root, dispList);
	root = doRightRotation(root);
      }
    }

    if(root->parent != NULL && isLeft) {
      dbg(20) << "root : " <<  toString(root) << " parent : " << toString(root->parent) 
	<< "..setting left child of parent as :" << toString(root) << dendl;
      root->parent->left = root;
    }
    else if(root->parent != NULL && (!isLeft)) {
      dbg(20) << "root : " <<  toString(root) << " parent : " << toString(root->parent) << 
	"..setting right child of parent as :" << toString(root) << dendl;
      root->parent->right = root;
    }

    adjustSpanAndDisplace(tmp, dispList);
    adjustSpanAndDisplace(p, dispList);
    adjustSpanAndDisplace(root, dispList);
    return root;
  }

  IntervalTree::TreeNode* next(IntervalTree::TreeNode* node)
  {
    assert(node != NULL);

    TreeNode* rightTree = node->right, *next = NULL, *parent;
    if(rightTree) {
      next = rightTree->left;
      parent = rightTree;
      while(next) {
	parent = next;
	next = next->left;
      }

      return parent;
    }

    parent = node->parent;
    if(parent == NULL) {
      return node;
    }
    next = node;

    while(parent) {
      if((parent->left != NULL) && (parent->left == next)) {
	return parent;
      }
      next = parent;          
      parent = next->parent;
    }

    if(parent == NULL) {
      return node;
    }
    return parent;
  }


  IntervalTree::TreeNode* prev(TreeNode* node)
  {
    assert(node != NULL);

    TreeNode* leftTree = node->left, *next = NULL, *parent;
    if(leftTree) {
      next = leftTree->right;
      parent = leftTree;
      while(next) {
	parent = next;
	next = next->right;
      }
      dbg(25) <<"returning prev :" << toString(parent) << dendl;
      return parent;
    }

    parent = node->parent;
    if(parent == NULL) {
      dbg(25) <<"returning prev :" << toString(node) << dendl;
      return node;
    }
    next = node;
    while(parent) {
      if((parent->right != NULL) && parent->right ==  next) {
	return parent;
      }
      next = parent;
      parent = next->parent;
    }

    if(parent == NULL) {
      dbg(25) <<"returning prev :" << toString(node) << dendl;
      return node;
    }

    dbg(25) <<"returning prev :" << toString(parent) << dendl;
    return parent;
  }


  std::pair<IntervalTree::TreeNode*, IntervalTree::TreeNode*> getSpan(const TreeNode* node) const
  {
    assert(node != NULL);

    TreeNode* leftMostNode = node->left, *rightMostNode = node->right, *lastLeftNode = NULL, *lastRightNode = NULL;   
    while(leftMostNode) {
      lastLeftNode = leftMostNode;
      leftMostNode = leftMostNode->left;
    }

    while(rightMostNode) {
      lastRightNode = rightMostNode;
      rightMostNode = rightMostNode->right;
    }

    dbg(25) << "returning span :{" << toString(lastLeftNode) << " , " << toString(lastRightNode) << "}" << dendl;
    return std::make_pair<TreeNode*, TreeNode*>(lastLeftNode, lastRightNode);
  }


  bool isElementaryInterval(const value_type&  value) const
  {
    TreeNode* current = root;
    while(current) {
      if(current->value == value) {
	return true;
      }

      if(value > current->value) {
	current = current->right;
      }
      else {
	current = current->left;
      }
    }

    return false;
  }

};


template<typename U, typename T>
void IntervalTree<U,T>::TreeNode::adjustHeight()
{
  if(this->right == NULL && this->left == NULL) {
    this->height =0;
    return;
  }
  this->height =  max (this->left? left->height: 0 , this->right? right->height:0) + 1  ;         
}


template<typename U, typename T>
void IntervalTree<U,T>::TreeNode::addNode(TreeNode* node, Child which)
{
  assert(node);

  if(which == LEFT) {
    this->left = node;
  }
  else {
    this->right = node;
  }

  node->parent = this;
  this->adjustHeight();
}

template<typename U, typename T>
void IntervalTree<U,T>::IntervalRange::printRange(std::ostream& out) const
{
  out << "interval range {" << this->start << "," << this->end << "}:::" ;
  typename std::list<IntervalNode>::const_iterator i;
  typename std::list<IntervalNode>::const_iterator iEnd = this->intervalNodeSet.end();  

  for(i = this->intervalNodeSet.begin(); i != iEnd; ++i) {
    out <<"{start :" <<(*i).start <<", end:" << (*i).end << "} {data: " << (*i).node << "}";
  }
  out << std::endl;
}


template<typename U, typename T>
int IntervalTree<U,T>::TreeNode::getBalance() const
{
  int bal =0;
  if(left != NULL) {
    bal = left->height;
  }

  if(right != NULL) {
    bal -= right->height;
  }

  return bal;
}


template<typename U, typename T>
void IntervalTree<U,T>::TreeNode::printNodes(std::ostream& out) const
{
  typename std::list<IntervalRange>::const_iterator i;
  typename std::list<IntervalRange>::const_iterator iEnd = this->rangeList.end();

  out << "node list :::" << std::endl;
  for(i = this->rangeList.begin(); i != iEnd; ++i) {
    (*i).printRange(out);
  }
  out << std::endl;
}


#endif
