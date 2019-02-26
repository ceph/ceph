import { Component, Input, OnInit } from '@angular/core';

import * as _ from 'lodash';

import { CdTableAction } from '../../models/cd-table-action';
import { CdTableSelection } from '../../models/cd-table-selection';
import { Permission } from '../../models/permissions';

@Component({
  selector: 'cd-table-actions',
  templateUrl: './table-actions.component.html',
  styleUrls: ['./table-actions.component.scss']
})
export class TableActionsComponent implements OnInit {
  @Input()
  permission: Permission;
  @Input()
  selection: CdTableSelection;
  @Input()
  tableActions: CdTableAction[];

  // Use this if you just want to display a drop down button,
  // labeled with the given text, with all actions in it.
  // This disables the main action button.
  @Input()
  onlyDropDown?: string;

  // Array with all visible actions
  dropDownActions: CdTableAction[] = [];

  constructor() {}

  ngOnInit() {
    this.removeActionsWithNoPermissions();
    this.updateDropDownActions();
  }

  toClassName(name: string): string {
    return name
      .replace(/ /g, '-')
      .replace(/[^a-z-]/gi, '')
      .toLowerCase();
  }

  /**
   * Removes all actions from 'tableActions' that need a permission the user doesn't have.
   */
  private removeActionsWithNoPermissions() {
    if (!this.permission) {
      this.tableActions = [];
      return;
    }
    const permissions = Object.keys(this.permission).filter((key) => this.permission[key]);
    this.tableActions = this.tableActions.filter((action) =>
      permissions.includes(action.permission)
    );
  }

  private updateDropDownActions() {
    this.dropDownActions = this.tableActions.filter((action) =>
      action.visible ? action.visible(this.selection) : action
    );
  }

  /**
   * Finds the next action that is used as main action for the button
   *
   * The order of the list is crucial to get the right main action.
   *
   * Default button conditions of actions:
   * - 'create' actions can be used with no or multiple selections
   * - 'update' and 'delete' actions can be used with one selection
   *
   * @returns {CdTableAction}
   */
  getCurrentButton(): CdTableAction {
    if (this.onlyDropDown) {
      return;
    }
    let buttonAction = this.dropDownActions.find((tableAction) => this.showableAction(tableAction));
    if (!buttonAction && this.dropDownActions.length > 0) {
      buttonAction = this.dropDownActions[0];
    }
    return buttonAction;
  }

  /**
   * Determines if action can be used for the button
   *
   * @param {CdTableAction} action
   * @returns {boolean}
   */
  private showableAction(action: CdTableAction): boolean {
    const condition = action.canBePrimary;
    const singleSelection = this.selection.hasSingleSelection;
    const defaultCase = action.permission === 'create' ? !singleSelection : singleSelection;
    return (condition && condition(this.selection)) || (!condition && defaultCase);
  }

  useRouterLink(action: CdTableAction): string {
    if (!action.routerLink || this.disableSelectionAction(action)) {
      return;
    }
    return _.isString(action.routerLink) ? action.routerLink : action.routerLink();
  }

  /**
   * Determines if an action should be disabled
   *
   * Default disable conditions of 'update' and 'delete' actions:
   * - If no or multiple selections are made
   * - If one selection is made, but a task is executed on that item
   *
   * @param {CdTableAction} action
   * @returns {Boolean}
   */
  disableSelectionAction(action: CdTableAction): Boolean {
    const permission = action.permission;
    const disable = action.disable;
    if (disable) {
      return Boolean(disable(this.selection));
    }
    const selected = this.selection.hasSingleSelection && this.selection.first();
    return Boolean(
      ['update', 'delete'].includes(permission) && (!selected || selected.cdExecuting)
    );
  }

  showDropDownActions() {
    this.updateDropDownActions();
    return this.dropDownActions.length > 1;
  }

  useClickAction(action: CdTableAction) {
    return action.click && action.click();
  }
}
