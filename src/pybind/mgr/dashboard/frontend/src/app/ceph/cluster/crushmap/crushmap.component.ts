import { Component, OnInit } from '@angular/core';

import { NodeEvent, TreeModel } from 'ng2-tree';

import { HealthService } from '../../../shared/api/health.service';

@Component({
  selector: 'cd-crushmap',
  templateUrl: './crushmap.component.html',
  styleUrls: ['./crushmap.component.scss']
})
export class CrushmapComponent implements OnInit {
  tree: TreeModel;
  metadata: any;
  metadataTitle: string;
  metadataKeyMap: { [key: number]: number } = {};

  constructor(private healthService: HealthService) {}

  ngOnInit() {
    this.healthService.getFullHealth().subscribe((data: any) => {
      this.tree = this._abstractTreeData(data);
    });
  }

  _abstractTreeData(data: any): TreeModel {
    const nodes = data.osd_map.tree.nodes || [];
    const treeNodeMap: { [key: number]: any } = {};

    if (0 === nodes.length) {
      return {
        value: 'No nodes!',
        settings: { static: true }
      };
    }

    const rootNodeId = nodes[0].id || null;
    nodes.reverse().forEach((node) => {
      treeNodeMap[node.id] = this.generateTreeLeaf(node, treeNodeMap);
    });

    return treeNodeMap[rootNodeId];
  }

  private generateTreeLeaf(node: any, treeNodeMap) {
    const id = node.id;
    this.metadataKeyMap[id] = node;
    const settings = { static: true };

    const value: string = node.name + ' (' + node.type + ')';
    const status: string = node.status;

    const children: any[] = [];
    if (node.children) {
      node.children.sort().forEach((childId) => {
        children.push(treeNodeMap[childId]);
      });

      return { value, status, settings, id, children };
    }

    return { value, status, settings, id };
  }

  onNodeSelected(e: NodeEvent) {
    const { name, type, status, ...remain } = this.metadataKeyMap[e.node.id];
    this.metadata = remain;
    this.metadataTitle = name + ' (' + type + ')';
  }
}
