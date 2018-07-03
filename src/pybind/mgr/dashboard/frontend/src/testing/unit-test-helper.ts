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
    this.testSingleScenario(
      [{}, {}],
      'multiple selections',
      fn,
      _.isUndefined(multiple) ? empty : multiple
    );
    this.testSingleScenario(
      [{ cdExecuting: 'someAction' }],
      'select executing item',
      fn,
      _.isUndefined(singleExecuting) ? single : singleExecuting
    );
    this.testSingleScenario([{}], 'select non-executing item', fn, single);
    this.testSingleScenario([], 'no selection', fn, empty);
  }

  private testSingleScenario(
    selection: object[],
    description: string,
    fn: () => any,
    expected: any
  ) {
    this.setSelection(selection);
    this.readableExpect(description, fn, expected);
  }

  setSelection(selection: object[]) {
    this.tableActions.selection.selected = selection;
    this.tableActions.selection.update();
  }

  private readableExpect(task: string, fn: () => any, expected: any) {
    expect({ task: task, expected: fn() }).toEqual({
      task: task,
      expected: expected
    });
  }
}
