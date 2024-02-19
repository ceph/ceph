import { TestBed } from '@angular/core/testing';

import { TreeViewService } from './tree-view.service';
import { Node } from 'carbon-components-angular/treeview/tree-node.types';
import _ from 'lodash';

describe('TreeViewService', () => {
  let service: TreeViewService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(TreeViewService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  describe('expandNode', () => {
    it('should expand the given node and its ancestors', () => {
      const nodes: Node[] = [
        {
          id: '1',
          label: 'Root',
          value: { parent: null },
          children: [
            {
              id: '2',
              label: 'Child 1',
              value: { parent: '1' },
              children: [
                {
                  id: '3',
                  label: 'Sub-child 1',
                  value: { parent: '2' }
                }
              ]
            }
          ]
        }
      ];

      const nodeToExpand: Node = nodes[0].children[0].children[0];
      const expandedNodes = service.expandNode(nodes, nodeToExpand);

      expect(expandedNodes[0].children[0].children[0].expanded).toBe(true);
      expect(expandedNodes[0].children[0].expanded).toBe(true);
      expect(expandedNodes[0].expanded).toBe(true);
    });

    it('should return a new array with the expanded nodes', () => {
      const nodes: Node[] = [
        {
          id: '1',
          label: 'Root',
          value: { parent: null },
          children: [
            {
              id: '2',
              label: 'Child 1',
              value: { parent: '1' },
              children: [
                {
                  id: '3',
                  label: 'Sub-child 1',
                  value: { parent: '2' }
                }
              ]
            }
          ]
        }
      ];

      const nodeToExpand: Node = nodes[0].children[0].children[0];
      const expandedNodes = service.expandNode(nodes, nodeToExpand);

      expect(nodes).not.toBe(expandedNodes);
    });

    it('should not modify the original nodes array', () => {
      const nodes: Node[] = [
        {
          id: '1',
          label: 'Root',
          value: { parent: null },
          children: [
            {
              id: '2',
              label: 'Child 1',
              value: { parent: '1' },
              children: [
                {
                  id: '3',
                  label: 'Sub-child 1',
                  value: { parent: '2' }
                }
              ]
            }
          ]
        }
      ];

      const nodeToExpand: Node = nodes[0].children[0].children[0];
      const originalNodesDeepCopy = _.cloneDeep(nodes); // create a deep copy of the nodes array

      service.expandNode(nodes, nodeToExpand);

      // Check that the original nodes array has not been modified
      expect(nodes).toEqual(originalNodesDeepCopy);
    });
  });

  describe('findNode', () => {
    it('should find a node by its id', () => {
      const nodes: Node[] = [
        { id: '1', label: 'Node 1', children: [] },
        { id: '2', label: 'Node 2', children: [{ id: '3', label: 'Node 3', children: [] }] }
      ];

      const foundNode = service.findNode('3', nodes);

      expect(foundNode).not.toBeNull();
      expect(foundNode?.id).toEqual('3');
      expect(foundNode?.label).toEqual('Node 3');
    });

    it('should return null if the node is not found', () => {
      const nodes: Node[] = [
        { id: '1', label: 'Node 1', children: [] },
        { id: '2', label: 'Node 2', children: [] }
      ];

      const foundNode = service.findNode('3', nodes);

      expect(foundNode).toBeNull();
    });

    it('should find a node by a custom property', () => {
      const nodes: Node[] = [
        { id: '1', label: 'Node 1', value: { customProperty: 'value1' }, children: [] },
        { id: '2', label: 'Node 2', value: { customProperty: 'value2' }, children: [] }
      ];

      const foundNode = service.findNode('value2', nodes, 'value.customProperty');

      expect(foundNode).not.toBeNull();
      expect(foundNode?.id).toEqual('2');
      expect(foundNode?.label).toEqual('Node 2');
    });

    it('should find a node by a custom property in children array', () => {
      const nodes: Node[] = [
        { id: '1', label: 'Node 1', value: { customProperty: 'value1' }, children: [] },
        {
          id: '2',
          label: 'Node 2',
          children: [{ id: '2.1', label: 'Node 2.1', value: { customProperty: 'value2.1' } }]
        }
      ];

      const foundNode = service.findNode('value2.1', nodes, 'value.customProperty');

      expect(foundNode).not.toBeNull();
      expect(foundNode?.id).toEqual('2.1');
      expect(foundNode?.label).toEqual('Node 2.1');
    });
  });
});
