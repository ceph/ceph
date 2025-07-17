import { CUSTOM_ELEMENTS_SCHEMA, NO_ERRORS_SCHEMA } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { AbstractControl } from '@angular/forms';

import _ from 'lodash';

import { TableActionsComponent } from '../datatable/table-actions/table-actions.component';
import { CdFormGroup } from '../forms/cd-form-group';
import { CdTableAction } from '../models/cd-table-action';
import { CdTableSelection } from '../models/cd-table-selection';
import { Permission } from '../models/permissions';

export function configureTestBed(configuration: any) {
  beforeEach(() => {
    TestBed.configureTestingModule({
      ...configuration,
      schemas: [NO_ERRORS_SCHEMA, CUSTOM_ELEMENTS_SCHEMA]
    });
  });
}

export class PermissionHelper {
  tac: TableActionsComponent;
  permission: Permission;
  selection: { single: object; multiple: object[] };

  /**
   * @param permission The permissions used by this test.
   * @param selection The selection used by this test. Configure this if
   *   the table actions require a more complex selection object to perform
   *   a correct test run.
   *   Defaults to `{ single: {}, multiple: [{}, {}] }`.
   */
  constructor(permission: Permission, selection?: { single: object; multiple: object[] }) {
    this.permission = permission;
    this.selection = _.defaultTo(selection, { single: {}, multiple: [{}, {}] });
  }

  setPermissionsAndGetActions(tableActions: CdTableAction[]): any {
    const result = {};
    [true, false].forEach((create) => {
      [true, false].forEach((update) => {
        [true, false].forEach((deleteP) => {
          this.permission.create = create;
          this.permission.update = update;
          this.permission.delete = deleteP;

          this.tac = new TableActionsComponent();
          this.tac.selection = new CdTableSelection();
          this.tac.tableActions = [...tableActions];
          this.tac.permission = this.permission;
          this.tac.ngOnInit();

          const perms = [];
          if (create) {
            perms.push('create');
          }
          if (update) {
            perms.push('update');
          }
          if (deleteP) {
            perms.push('delete');
          }
          const permissionText = perms.join(',');

          result[permissionText !== '' ? permissionText : 'no-permissions'] = {
            actions: this.tac.tableActions.map((action) => action.name),
            primary: this.testScenarios()
          };
        });
      });
    });

    return result;
  }

  testScenarios() {
    const result: any = {};
    // 'multiple selections'
    result.multiple = this.testScenario(this.selection.multiple);
    // 'select executing item'
    result.executing = this.testScenario([
      _.merge({ cdExecuting: 'someAction' }, this.selection.single)
    ]);
    // 'select non-executing item'
    result.single = this.testScenario([this.selection.single]);
    // 'no selection'
    result.no = this.testScenario([]);

    return result;
  }

  private testScenario(selection: object[]) {
    this.setSelection(selection);
    const action: CdTableAction = this.tac.currentAction;
    return action ? action.name : '';
  }

  setSelection(selection: object[]) {
    this.tac.selection.selected = selection;
    this.tac.onSelectionChange();
  }
}

export class FormHelper {
  form: CdFormGroup;

  constructor(form: CdFormGroup) {
    this.form = form;
  }

  /**
   * Changes multiple values in multiple controls
   */
  setMultipleValues(values: { [controlName: string]: any }, markAsDirty?: boolean) {
    Object.keys(values).forEach((key) => {
      this.setValue(key, values[key], markAsDirty);
    });
  }

  /**
   * Changes the value of a control
   */
  setValue(control: AbstractControl | string, value: any, markAsDirty?: boolean): AbstractControl {
    control = this.getControl(control);
    if (markAsDirty) {
      control.markAsDirty();
    }
    control.setValue(value);
    return control;
  }

  private getControl(control: AbstractControl | string): AbstractControl {
    if (typeof control === 'string') {
      return this.form.get(control);
    }
    return control;
  }

  /**
   * Change the value of the control and expect the control to be valid afterwards.
   */
  expectValidChange(control: AbstractControl | string, value: any, markAsDirty?: boolean) {
    this.expectValid(this.setValue(control, value, markAsDirty));
  }

  /**
   * Expect that the given control is valid.
   */
  expectValid(control: AbstractControl | string) {
    // 'isValid' would be false for disabled controls
    expect(this.getControl(control).errors).toBe(null);
  }

  /**
   * Change the value of the control and expect a specific error.
   */
  expectErrorChange(
    control: AbstractControl | string,
    value: any,
    error: string,
    markAsDirty?: boolean
  ) {
    this.expectError(this.setValue(control, value, markAsDirty), error);
  }

  /**
   * Expect a specific error for the given control.
   */
  expectError(control: AbstractControl | string, error: string) {
    expect(this.getControl(control).hasError(error)).toBeTruthy();
  }
}
