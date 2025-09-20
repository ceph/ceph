import { Component, OnDestroy, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { TreeViewComponent } from 'carbon-components-angular';
import { Node } from 'carbon-components-angular/treeview/tree-node.types';
import { Observable, Subscription } from 'rxjs';

import { CrushRuleService } from '~/app/shared/api/crush-rule.service';
import { Icons } from '~/app/shared/enum/icons.enum';
import { TimerService } from '~/app/shared/services/timer.service';

export interface CrushmapInfo {
  names: string[];
  nodes: CrushmapNode[];
  roots: number[];
  [key: string]: any;
}

export interface CrushmapNode {
  id: number;
  name: string;
  type?: string;
  type_id: number;
  children?: number[];
  pool_weights?: Record<string, any>;
  device_class?: string;
  crush_weight?: number;
  depth?: number;
  exists?: number;
  status?: string;
  reweight?: number;
  primary_affinity?: number;
  [key: string]: any;
}

@Component({
  selector: 'cd-crushmap',
  templateUrl: './crushmap.component.html',
  styleUrls: ['./crushmap.component.scss']
})
export class CrushmapComponent implements OnDestroy, OnInit {
  private sub = new Subscription();

  @ViewChild('tree') tree: TreeViewComponent;
  @ViewChild('badge') labelTpl: TemplateRef<any>;

  icons = Icons;
  loadingIndicator = true;
  nodes: Node[] = [];
  metadata: any;
  metadataTitle: string;
  metadataKeyMap: { [key: number]: any } = {};
  data$: Observable<object>;

  constructor(private crushRuleService: CrushRuleService, private timerService: TimerService) {}

  ngOnInit() {
    this.sub = this.timerService
      .get(() => this.crushRuleService.getInfo(), 5000)
      .subscribe((data: CrushmapInfo) => {
        this.loadingIndicator = false;
        this.nodes = this.abstractTreeData(data);
      });
  }

  ngOnDestroy() {
    this.sub.unsubscribe();
  }

  private abstractTreeData(data: CrushmapInfo): Node[] {
    const nodes = data.nodes || [];
    const rootNodes = data.roots || [];
    const treeNodeMap: { [key: number]: any } = {};

    if (0 === nodes.length) {
      return [
        {
          label: 'No nodes!'
        }
      ];
    }

    const roots: any[] = [];
    nodes.reverse().forEach((node: CrushmapNode) => {
      if (rootNodes.includes(node.id)) {
        roots.push(node.id);
      }
      treeNodeMap[node.id] = this.generateTreeLeaf(node, treeNodeMap);
    });

    const children = roots.map((id) => {
      return treeNodeMap[id];
    });

    return children;
  }

  private generateTreeLeaf(node: CrushmapNode, treeNodeMap: Record<number, any>) {
    const cdId = node.id;
    this.metadataKeyMap[cdId] = node;

    const name: string = node.name + ' (' + node.type + ')';
    const status: string = node.status;

    const children: any[] = [];
    const resultNode: Record<string, any> = {
      label: this.labelTpl,
      labelContext: { name, status, type: node?.type },
      value: name,
      id: cdId,
      expanded: true,
      name,
      status,
      cdId,
      type: node.type
    };
    if (node?.children?.length) {
      node.children.sort().forEach((childId: number) => {
        children.push(treeNodeMap[childId]);
      });

      resultNode['children'] = children;
    }

    return resultNode;
  }

  onNodeSelected(node: Node) {
    if (node.id !== undefined) {
      const { name, type, status, ...remain } = this.metadataKeyMap[Number(node.id)];
      this.metadata = remain;
      this.metadataTitle = name + ' (' + type + ')';
    } else {
      delete this.metadata;
      delete this.metadataTitle;
    }
  }
}
