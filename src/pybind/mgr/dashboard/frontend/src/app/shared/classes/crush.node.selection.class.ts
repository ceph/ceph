import { AbstractControl } from '@angular/forms';

import * as _ from 'lodash';

import { CrushNode } from '../models/crush-node';

export class CrushNodeSelectionClass {
  private nodes: CrushNode[] = [];
  private easyNodes: { [id: number]: CrushNode } = {};
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

  initCrushNodeSelection(
    nodes: CrushNode[],
    rootControl: AbstractControl,
    failureControl: AbstractControl,
    deviceControl: AbstractControl
  ) {
    this.nodes = nodes;
    nodes.forEach((node) => {
      this.easyNodes[node.id] = node;
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
    const nodes = this.getSubNodes(this.controls.root.value);
    const domains = {};
    nodes.forEach((node) => {
      if (!domains[node.type]) {
        domains[node.type] = [];
      }
      domains[node.type].push(node);
    });
    Object.keys(domains).forEach((type) => {
      if (domains[type].length <= 1) {
        delete domains[type];
      }
    });
    this.failureDomains = domains;
    this.failureDomainKeys = Object.keys(domains).sort();
    this.updateFailureDomain();
  }

  private getSubNodes(node: CrushNode): CrushNode[] {
    let subNodes = [node]; // Includes parent node
    if (!node.children) {
      return subNodes;
    }
    node.children.forEach((id) => {
      const childNode = this.easyNodes[id];
      subNodes = subNodes.concat(this.getSubNodes(childNode));
    });
    return subNodes;
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
      this.failureDomains[failureDomain].map((node) => this.getSubNodes(node))
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
