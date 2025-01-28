import { Injectable } from '@angular/core';
import _ from 'lodash';
import { Node } from 'carbon-components-angular/treeview/tree-node.types';

@Injectable({
  providedIn: 'root'
})
export class TreeViewService {
  constructor() {}

  /**
   * Finds a node in a given nodes array
   * @param value Value you want to match against
   * @param nodes The Node[] array to search into
   * @param property Property to match value against. default is 'id'
   * @returns Node object if is found or null otherwise
   */
  findNode<T>(value: T, nodes: Node[], property = 'id'): Node | null {
    let result: Node | null = null;
    nodes.some(
      (node: Node) =>
        (result =
          _.get(node, property) === value
            ? node
            : this.findNode(value, node.children || [], property))
    );
    return result;
  }

  /**
   * Expands node and its ancestors
   * @param nodeCopy Nodes that make up the tree component
   * @param nodeToExpand Node to be expanded
   * @returns New list of nodes with expand persisted
   */
  expandNode(nodes: Node[], nodeToExpand: Node): Node[] {
    const nodesCopy = _.cloneDeep(nodes);
    const expand = (tree: Node[], nodeToExpand: Node) =>
      tree.map((node) => {
        if (node.id === nodeToExpand.id) {
          return { ...node, expanded: true };
        } else if (node.children) {
          node.children = expand(node.children, nodeToExpand);
        }
        return node;
      });

    let expandedNodes = expand(nodesCopy, nodeToExpand);
    let parent = this.findNode(nodeToExpand?.value?.parent, nodesCopy);

    while (parent) {
      expandedNodes = expand(expandedNodes, parent);
      parent = this.findNode(parent?.value?.parent, nodesCopy);
    }

    return expandedNodes;
  }
}
