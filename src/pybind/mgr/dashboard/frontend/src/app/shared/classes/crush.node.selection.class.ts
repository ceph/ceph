import { AbstractControl } from '@angular/forms';

import * as _ from 'lodash';

import { CrushNode } from '../models/crush-node';

export class CrushNodeSelectionClass {
  private nodes: CrushNode[] = [];
  private idTree: { [id: number]: CrushNode } = {};
  private allDevices: string[] = [];
  private controls: {
    root: AbstractControl;
    failure: AbstractControl;
    device: AbstractControl;
  };

  buckets: CrushNode[] = [];
  failureDomains: { [type: string]: CrushNode[] } = {};
  failureDomainKeys: string[] = [];
  devices: string[] = [];
  deviceCount = 0;

  static searchFailureDomains(
    nodes: CrushNode[],
    s: string
  ): { [failureDomain: string]: CrushNode[] } {
    return this.getFailureDomains(this.search(nodes, s));
  }

  /**
   * Filters crush map for a node and it's tree.
   * The node name as provided in crush rules attribute item_name is supported.
   * This means that '$name~$deviceType' can be used and will result in a crush map
   * that only include buckets with the specified device in use as their leaf.
   */
  static search(nodes: CrushNode[], s: string): CrushNode[] {
    const [search, deviceType] = s.split('~'); // Used inside item_name in crush rules
    const node = nodes.find((n) => ['name', 'id', 'type'].some((attr) => n[attr] === search));
    if (!node) {
      return [];
    }
    nodes = this.getSubNodes(node, this.createIdTreeFromNodes(nodes));
    if (deviceType) {
      nodes = this.filterNodesByDeviceType(nodes, deviceType);
    }
    return nodes;
  }

  static createIdTreeFromNodes(nodes: CrushNode[]): { [id: number]: CrushNode } {
    const idTree = {};
    nodes.forEach((node) => {
      idTree[node.id] = node;
    });
    return idTree;
  }

  static getSubNodes(node: CrushNode, idTree: { [id: number]: CrushNode }): CrushNode[] {
    let subNodes = [node]; // Includes parent node
    if (!node.children) {
      return subNodes;
    }
    node.children.forEach((id) => {
      const childNode = idTree[id];
      subNodes = subNodes.concat(this.getSubNodes(childNode, idTree));
    });
    return subNodes;
  }

  static filterNodesByDeviceType(nodes: CrushNode[], deviceType: string): any {
    let doNotInclude = nodes
      .filter((n) => n.device_class && n.device_class !== deviceType)
      .map((n) => n.id);
    let foundNewNode: boolean;
    let childrenToRemove = doNotInclude;

    // Filters out all unwanted nodes
    do {
      foundNewNode = false;
      nodes = nodes.filter((n) => !doNotInclude.includes(n.id)); // Unwanted nodes
      // Find nodes where all children were filtered
      const toRemoveNext: number[] = [];
      nodes.forEach((n) => {
        if (n.children && n.children.every((id) => doNotInclude.includes(id))) {
          toRemoveNext.push(n.id);
          foundNewNode = true;
        }
      });
      if (foundNewNode) {
        doNotInclude = toRemoveNext; // Reduces array length
        childrenToRemove = childrenToRemove.concat(toRemoveNext);
      }
    } while (foundNewNode);

    // Removes filtered out children in all left nodes with children
    nodes = _.cloneDeep(nodes); // Clone objects to not change original objects
    nodes = nodes.map((n) => {
      if (!n.children) {
        return n;
      }
      n.children = n.children.filter((id) => !childrenToRemove.includes(id));
      return n;
    });

    return nodes;
  }

  static getFailureDomains(nodes: CrushNode[]): { [failureDomain: string]: CrushNode[] } {
    const domains = {};
    nodes.forEach((node) => {
      const type = node.type;
      if (!domains[type]) {
        domains[type] = [];
      }
      domains[type].push(node);
    });
    return domains;
  }

  initCrushNodeSelection(
    nodes: CrushNode[],
    rootControl: AbstractControl,
    failureControl: AbstractControl,
    deviceControl: AbstractControl
  ) {
    this.nodes = nodes;
    this.idTree = CrushNodeSelectionClass.createIdTreeFromNodes(nodes);
    nodes.forEach((node) => {
      this.idTree[node.id] = node;
    });
    this.buckets = _.sortBy(
      nodes.filter((n) => n.children),
      'name'
    );
    this.controls = {
      root: rootControl,
      failure: failureControl,
      device: deviceControl
    };
    this.preSelectRoot();
    this.controls.root.valueChanges.subscribe(() => this.onRootChange());
    this.controls.failure.valueChanges.subscribe(() => this.onFailureDomainChange());
    this.controls.device.valueChanges.subscribe(() => this.onDeviceChange());
  }

  private preSelectRoot() {
    const rootNode = this.nodes.find((node) => node.type === 'root');
    this.silentSet(this.controls.root, rootNode);
    this.onRootChange();
  }

  private silentSet(control: AbstractControl, value: any) {
    control.setValue(value, { emitEvent: false });
  }

  private onRootChange() {
    const nodes = CrushNodeSelectionClass.getSubNodes(this.controls.root.value, this.idTree);
    const domains = CrushNodeSelectionClass.getFailureDomains(nodes);
    Object.keys(domains).forEach((type) => {
      if (domains[type].length <= 1) {
        delete domains[type];
      }
    });
    this.failureDomains = domains;
    this.failureDomainKeys = Object.keys(domains).sort();
    this.updateFailureDomain();
  }

  private updateFailureDomain() {
    let failureDomain = this.getIncludedCustomValue(
      this.controls.failure,
      Object.keys(this.failureDomains)
    );
    if (failureDomain === '') {
      failureDomain = this.setMostCommonDomain(this.controls.failure);
    }
    this.updateDevices(failureDomain);
  }

  private getIncludedCustomValue(control: AbstractControl, includedIn: string[]) {
    return control.dirty && includedIn.includes(control.value) ? control.value : '';
  }

  private setMostCommonDomain(failureControl: AbstractControl): string {
    let winner = { n: 0, type: '' };
    Object.keys(this.failureDomains).forEach((type) => {
      const n = this.failureDomains[type].length;
      if (winner.n < n) {
        winner = { n, type };
      }
    });
    this.silentSet(failureControl, winner.type);
    return winner.type;
  }

  private onFailureDomainChange() {
    this.updateDevices();
  }

  private updateDevices(failureDomain: string = this.controls.failure.value) {
    const subNodes = _.flatten(
      this.failureDomains[failureDomain].map((node) =>
        CrushNodeSelectionClass.getSubNodes(node, this.idTree)
      )
    );
    this.allDevices = subNodes.filter((n) => n.device_class).map((n) => n.device_class);
    this.devices = _.uniq(this.allDevices).sort();
    const device =
      this.devices.length === 1
        ? this.devices[0]
        : this.getIncludedCustomValue(this.controls.device, this.devices);
    this.silentSet(this.controls.device, device);
    this.onDeviceChange(device);
  }

  private onDeviceChange(deviceType: string = this.controls.device.value) {
    this.deviceCount =
      deviceType === ''
        ? this.allDevices.length
        : this.allDevices.filter((type) => type === deviceType).length;
  }
}
