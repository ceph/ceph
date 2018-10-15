import { async, TestBed } from '@angular/core/testing';

import * as _ from 'lodash';

import { TableActionsComponent } from '../app/shared/datatable/table-actions/table-actions.component';
import { Permission } from '../app/shared/models/permissions';
import { _DEV_ } from '../unit-test-configuration';

export function configureTestBed(configuration, useOldMethod?) {
  if (_DEV_ && !useOldMethod) {
    const resetTestingModule = TestBed.resetTestingModule;
    beforeAll((done) =>
      (async () => {
        TestBed.resetTestingModule();
        TestBed.configureTestingModule(configuration);
        // prevent Angular from resetting testing module
        TestBed.resetTestingModule = () => TestBed;
      })()
        .then(done)
        .catch(done.fail));
    afterAll(() => {
      TestBed.resetTestingModule = resetTestingModule;
    });
  } else {
    beforeEach(async(() => {
      TestBed.configureTestingModule(configuration);
    }));
  }
}

export class PermissionHelper {
  tableActions: TableActionsComponent;
  permission: Permission;
  getTableActionComponent: () => TableActionsComponent;

  constructor(permission: Permission, getTableActionComponent: () => TableActionsComponent) {
    this.permission = permission;
    this.getTableActionComponent = getTableActionComponent;
  }

  setPermissionsAndGetActions(
    createPerm: number | boolean,
    updatePerm: number | boolean,
    deletePerm: number | boolean
  ): TableActionsComponent {
    this.permission.create = Boolean(createPerm);
    this.permission.update = Boolean(updatePerm);
    this.permission.delete = Boolean(deletePerm);
    this.tableActions = this.getTableActionComponent();
    return this.tableActions;
  }

  testScenarios({
    fn,
    empty,
    single,
    singleExecuting,
    multiple
  }: {
    fn: () => any;
    empty: any;
    single: any;
    singleExecuting?: any; // uses 'single' if not defined
    multiple?: any; // uses 'empty' if not defined
  }) {
    this.testScenario( // 'multiple selections'
      [{}, {}],
      fn,
      _.isUndefined(multiple) ? empty : multiple
    );
    this.testScenario( // 'select executing item'
      [{ cdExecuting: 'someAction' }],
      fn,
      _.isUndefined(singleExecuting) ? single : singleExecuting
    );
    this.testScenario([{}], fn, single); // 'select non-executing item'
    this.testScenario([], fn, empty); // 'no selection'
  }

  private testScenario(
    selection: object[],
    fn: () => any,
    expected: any
  ) {
    this.setSelection(selection);
    expect(fn()).toBe(expected);
  }

  setSelection(selection: object[]) {
    this.tableActions.selection.selected = selection;
    this.tableActions.selection.update();
  }
}
