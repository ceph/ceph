import { Component, OnInit } from '@angular/core';

import { NodeEvent, TreeModel } from 'ng2-tree';

import { DashboardService } from '../../../shared/api/dashboard.service';

@Component({
  selector: 'cd-crushmap',
  templateUrl: './crushmap.component.html',
  styleUrls: ['./crushmap.component.scss']
})
export class CrushmapComponent implements OnInit {
  panelTitle: string;
  tree: TreeModel;
  metadata: any;
  metadataKeyMap: { [key: number]: number } = {};

  constructor(private dashboardService: DashboardService) {
    this.panelTitle = 'CRUSH map viewer';
  }

  ngOnInit() {
    this.dashboardService.getHealth().subscribe((data: any) => {
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

    let value: string = node.name + ' (' + node.type + ')';
    if (node.status) {
      value += '--' + node.status;
    }

    const children: any[] = [];
    if (node.children) {
      node.children.sort().forEach((childId) => {
        children.push(treeNodeMap[childId]);
      });

      return { value, settings, id, children };
    }

    return { value, settings, id };
  }

  onNodeSelected(e: NodeEvent) {
    this.metadata = this.metadataKeyMap[e.node.id];
  }
}
