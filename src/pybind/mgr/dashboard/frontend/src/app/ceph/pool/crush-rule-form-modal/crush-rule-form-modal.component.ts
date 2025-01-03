import { Component, EventEmitter, OnInit, Output } from '@angular/core';
import { Validators } from '@angular/forms';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import _ from 'lodash';

import { CrushRuleService } from '~/app/shared/api/crush-rule.service';
import { CrushNodeSelectionClass } from '~/app/shared/classes/crush.node.selection.class';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { CrushNode } from '~/app/shared/models/crush-node';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';

@Component({
  selector: 'cd-crush-rule-form-modal',
  templateUrl: './crush-rule-form-modal.component.html',
  styleUrls: ['./crush-rule-form-modal.component.scss']
})
export class CrushRuleFormModalComponent extends CrushNodeSelectionClass implements OnInit {
  @Output()
  submitAction = new EventEmitter();

  tooltips = this.crushRuleService.formTooltips;

  form: CdFormGroup;
  names: string[];
  action: string;
  resource: string;

  constructor(
    private formBuilder: CdFormBuilder,
    public activeModal: NgbActiveModal,
    private taskWrapper: TaskWrapperService,
    private crushRuleService: CrushRuleService,
    public actionLabels: ActionLabelsI18n
  ) {
    super();
    this.action = this.actionLabels.CREATE;
    this.resource = $localize`Crush Rule`;
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
        this.initCrushNodeSelection(
          nodes,
          this.form.get('root'),
          this.form.get('failure_domain'),
          this.form.get('device_class')
        );
        this.names = names;
      });
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
      .subscribe({
        error: () => {
          this.form.setErrors({ cdSubmitButton: true });
        },
        complete: () => {
          this.activeModal.close();
          this.submitAction.emit(rule);
        }
      });
  }
}
