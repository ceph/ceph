import { Component, EventEmitter, OnInit, Output } from '@angular/core';
import { Validators } from '@angular/forms';

import { I18n } from '@ngx-translate/i18n-polyfill';
import * as _ from 'lodash';
import { BsModalRef } from 'ngx-bootstrap/modal';

import { CrushRuleService } from '../../../shared/api/crush-rule.service';
import { ActionLabelsI18n } from '../../../shared/constants/app.constants';
import { CdFormBuilder } from '../../../shared/forms/cd-form-builder';
import { CdFormGroup } from '../../../shared/forms/cd-form-group';
import { CdValidators } from '../../../shared/forms/cd-validators';
import { CrushNode } from '../../../shared/models/crush-node';
import { FinishedTask } from '../../../shared/models/finished-task';
import { TaskWrapperService } from '../../../shared/services/task-wrapper.service';

@Component({
  selector: 'cd-crush-rule-form-modal',
  templateUrl: './crush-rule-form-modal.component.html',
  styleUrls: ['./crush-rule-form-modal.component.scss']
})
export class CrushRuleFormModalComponent implements OnInit {
  @Output()
  submitAction = new EventEmitter();

  buckets: CrushNode[] = [];
  failureDomains: { [type: string]: CrushNode[] } = {};
  devices: string[] = [];
  tooltips = this.crushRuleService.formTooltips;

  form: CdFormGroup;
  names: string[];
  action: string;
  resource: string;

  private nodes: CrushNode[] = [];
  private easyNodes: { [id: number]: CrushNode } = {};

  constructor(
    private formBuilder: CdFormBuilder,
    public bsModalRef: BsModalRef,
    private taskWrapper: TaskWrapperService,
    private crushRuleService: CrushRuleService,
    private i18n: I18n,
    public actionLabels: ActionLabelsI18n
  ) {
    this.action = this.actionLabels.CREATE;
    this.resource = this.i18n('Crush Rule');
    this.createForm();
  }

  createForm() {
    this.form = this.formBuilder.group({
      // name: string
      name: [
        '',
        [
          Validators.required,
          Validators.pattern('[A-Za-z0-9_-]+'),
          CdValidators.custom(
            'uniqueName',
            (value: any) => this.names && this.names.indexOf(value) !== -1
          )
        ]
      ],
      // root: CrushNode
      root: null, // Replaced with first root
      // failure_domain: string
      failure_domain: '', // Replaced with most common type
      // device_class: string
      device_class: '' // Replaced with device type if only one exists beneath domain
    });
  }

  ngOnInit() {
    this.crushRuleService
      .getInfo()
      .subscribe(({ names, nodes }: { names: string[]; nodes: CrushNode[] }) => {
        this.nodes = nodes;
        nodes.forEach((node) => {
          this.easyNodes[node.id] = node;
        });
        this.buckets = _.sortBy(nodes.filter((n) => n.children), 'name');
        this.names = names;
        this.preSelectRoot();
      });
    this.form.get('root').valueChanges.subscribe((root: CrushNode) => this.updateRoot(root));
    this.form
      .get('failure_domain')
      .valueChanges.subscribe((domain: string) => this.updateDevices(domain));
  }

  private preSelectRoot() {
    const rootNode = this.nodes.find((node) => node.type === 'root');
    this.form.silentSet('root', rootNode);
    this.updateRoot(rootNode);
  }

  private updateRoot(rootNode: CrushNode) {
    const nodes = this.getSubNodes(rootNode);
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
      'failure_domain',
      Object.keys(this.failureDomains)
    );
    if (failureDomain === '') {
      failureDomain = this.setMostCommonDomain();
    }
    this.updateDevices(failureDomain);
  }

  private getIncludedCustomValue(controlName: string, includedIn: string[]) {
    const control = this.form.get(controlName);
    return control.dirty && includedIn.includes(control.value) ? control.value : '';
  }

  private setMostCommonDomain(): string {
    let winner = { n: 0, type: '' };
    Object.keys(this.failureDomains).forEach((type) => {
      const n = this.failureDomains[type].length;
      if (winner.n < n) {
        winner = { n, type };
      }
    });
    this.form.silentSet('failure_domain', winner.type);
    return winner.type;
  }

  updateDevices(failureDomain: string) {
    const subNodes = _.flatten(
      this.failureDomains[failureDomain].map((node) => this.getSubNodes(node))
    );
    this.devices = _.uniq(subNodes.filter((n) => n.device_class).map((n) => n.device_class)).sort();
    const device =
      this.devices.length === 1
        ? this.devices[0]
        : this.getIncludedCustomValue('device_class', this.devices);
    this.form.get('device_class').setValue(device);
  }

  failureDomainKeys(): string[] {
    return Object.keys(this.failureDomains).sort();
  }

  onSubmit() {
    if (this.form.invalid) {
      this.form.setErrors({ cdSubmitButton: true });
      return;
    }
    const rule = _.cloneDeep(this.form.value);
    rule.root = rule.root.name;
    if (rule.device_class === '') {
      delete rule.device_class;
    }
    this.taskWrapper
      .wrapTaskAroundCall({
        task: new FinishedTask('crushRule/create', rule),
        call: this.crushRuleService.create(rule)
      })
      .subscribe(
        undefined,
        () => {
          this.form.setErrors({ cdSubmitButton: true });
        },
        () => {
          this.bsModalRef.hide();
          this.submitAction.emit(rule);
        }
      );
  }
}
