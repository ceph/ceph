import { Component, Input, OnChanges, OnInit, SimpleChanges } from '@angular/core';

import _ from 'lodash';

import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Permission } from '~/app/shared/models/permissions';

@Component({
  selector: 'cd-table-actions',
  templateUrl: './table-actions.component.html',
  styleUrls: ['./table-actions.component.scss']
})
export class TableActionsComponent implements OnChanges, OnInit {
  @Input()
  permission: Permission;
  @Input()
  selection: CdTableSelection;
  @Input()
  tableActions: CdTableAction[];
  @Input()
  btnColor = 'accent';

  // Use this if you just want to display a drop down button,
  // labeled with the given text, with all actions in it.
  // This disables the main action button.
  @Input()
  dropDownOnly?: string;
  @Input()
  primaryDropDown = false;

  currentAction?: CdTableAction;
  // Array with all visible actions
  dropDownActions: CdTableAction[] = [];

  icons = Icons;

  ngOnInit() {
    this.removeActionsWithNoPermissions();
    this.onSelectionChange();
  }

  ngOnChanges(changes: SimpleChanges) {
    if (changes.selection) {
      this.onSelectionChange();
    }
  }

  onSelectionChange(): void {
    this.updateDropDownActions();
    this.updateCurrentAction();
  }

  toClassName(action: CdTableAction): string {
    return action.name
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

  private updateDropDownActions(): void {
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
   */
  private updateCurrentAction(): void {
    if (this.dropDownOnly) {
      this.currentAction = undefined;
      return;
    }
    /**
     * current action will always be the first action if that has a create permission
     * otherwise if there's only a single actions that will be the current action
     */
    if (this.dropDownActions?.[0]?.permission === 'create') {
      this.currentAction = this.dropDownActions[0];
    } else if (this.dropDownActions.length === 1) {
      this.currentAction = this.dropDownActions[0];
    }
  }

  useRouterLink(action: CdTableAction): string {
    if (!action.routerLink || this.disableSelectionAction(action)) {
      return undefined;
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
    const disable = action.disable;
    if (disable) {
      return Boolean(disable(this.selection));
    }
    const permission = action.permission;
    const selected = this.selection.hasSingleSelection && this.selection.first();
    return Boolean(
      ['update', 'delete'].includes(permission) && (!selected || selected.cdExecuting)
    );
  }

  useClickAction(action: CdTableAction) {
    /**
     * In order to show tooltips for deactivated menu items, the class
     * 'pointer-events: auto;' has been added to the .scss file which also
     * re-activates the click-event.
     * To prevent calling the click-event on deactivated elements we also have
     * to check here if it's disabled.
     */
    return !this.disableSelectionAction(action) && action.click && action.click();
  }

  useDisableDesc(action: CdTableAction) {
    if (action.disable) {
      const result = action.disable(this.selection);
      return _.isString(result) ? result : action.title ? action.title : undefined;
    } else if (action.title) {
      return action.title;
    }
    return undefined;
  }
}
