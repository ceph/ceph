import { Component, OnDestroy, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { TreeViewComponent } from 'carbon-components-angular';
import { Node } from '~/app/shared/models/carbon-tree-node';
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
  styleUrls: ['./crushmap.component.scss'],
  standalone: false
})
export class CrushmapComponent implements OnDestroy, OnInit {
  private sub = new Subscription();

  @ViewChild('tree') tree: TreeViewComponent;
  @ViewChild('tag') labelTpl: TemplateRef<any>;

  icons = Icons;
  loadingIndicator = true;
  nodes: Node[] = [];
  metadata: any;
  metadataTitle: string;
  metadataKeyMap: { [key: number]: any } = {};
  data$: Observable<object>;
  /**
   * IDs of nodes the user has collapsed. Kept across background refreshes so
   * that rebuilding the tree does not reset the expanded/collapsed state.
   * Unknown/new nodes default to expanded.
   */
  private collapsedNodeIds = new Set<number>();

  constructor(
    private crushRuleService: CrushRuleService,
    private timerService: TimerService
  ) {}

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

    // `nodes` comes from `ceph osd tree` and is already ordered the same way
    // as the CLI output: buckets by name before devices, devices by class and
    // then numerically by id. The `children` lists are not (they come reversed),
    // so order them by their position in `nodes`.
    const treeOrder = new Map<number, number>(nodes.map((node, index) => [node.id, index]));
    const byTreeOrder = (a: number, b: number) =>
      (treeOrder.get(a) ?? Infinity) - (treeOrder.get(b) ?? Infinity);

    // Children have to be built before their parents, so walk the list backwards.
    [...nodes].reverse().forEach((node: CrushmapNode) => {
      treeNodeMap[node.id] = this.generateTreeLeaf(node, treeNodeMap, byTreeOrder);
    });

    // `roots` (crush.find_roots()) is already sorted by id like the CLI, but
    // also contains shadow roots, which are not part of `nodes`.
    return rootNodes.filter((id) => treeNodeMap[id]).map((id) => treeNodeMap[id]);
  }

  private generateTreeLeaf(
    node: CrushmapNode,
    treeNodeMap: Record<number, any>,
    compareFn: (a: number, b: number) => number
  ) {
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
      expanded: !this.collapsedNodeIds.has(cdId),
      name,
      status,
      cdId,
      type: node.type
    };
    if (node?.children?.length) {
      [...node.children].sort(compareFn).forEach((childId: number) => {
        children.push(treeNodeMap[childId]);
      });

      resultNode['children'] = children;
    }

    return resultNode;
  }

  onNodeToggle(event: { node: Pick<Node, 'id'> }) {
    const id = Number(event?.node?.id);
    if (Number.isNaN(id)) {
      return;
    }
    if (this.collapsedNodeIds.has(id)) {
      this.collapsedNodeIds.delete(id);
    } else {
      this.collapsedNodeIds.add(id);
    }
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

  getStatusClasses(status: string): string {
    if (['in', 'up'].includes(status)) {
      return 'tag-success ';
    }
    if (['down', 'out', 'destroyed'].includes(status)) {
      return 'tag-danger';
    }
    return '';
  }
}
