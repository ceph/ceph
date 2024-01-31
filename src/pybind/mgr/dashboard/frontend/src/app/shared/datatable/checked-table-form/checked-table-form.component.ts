import { Component, Input, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { CdTableColumn } from '../../models/cd-table-column';
import { CdFormGroup } from '../../forms/cd-form-group';
import _ from 'lodash';

@Component({
  selector: 'cd-checked-table-form',
  templateUrl: './checked-table-form.component.html',
  styleUrls: ['./checked-table-form.component.scss']
})
export class CheckedTableFormComponent implements OnInit {
  @Input() data: Array<any>;
  @Input() columns: CdTableColumn[];
  @Input() form: CdFormGroup;
  @Input() inputField: string;
  @Input() scopes: Array<string> = [];
  @Input() isTableForOctalMode = false;
  @Input() initialValue = {};
  @Input() isDisabled = false;

  @ViewChild('headerPermissionCheckboxTpl', { static: true })
  headerPermissionCheckboxTpl: TemplateRef<any>;
  @ViewChild('cellScopeCheckboxTpl', { static: true })
  cellScopeCheckboxTpl: TemplateRef<any>;
  @ViewChild('cellPermissionCheckboxTpl', { static: true })
  cellPermissionCheckboxTpl: TemplateRef<any>;

  constructor() {}

  ngOnInit(): void {
    this.columns.forEach((column) => {
      if (column.name === 'All') {
        column.cellTemplate = this.cellScopeCheckboxTpl;
        column.headerTemplate = this.headerPermissionCheckboxTpl;
      } else {
        column.cellTemplate = this.cellPermissionCheckboxTpl;
        column.headerTemplate = this.headerPermissionCheckboxTpl;
      }
    });
    this.listenToChanges();
    this.form.get(this.inputField).setValue(this.initialValue);
  }

  listenToChanges() {
    // Create/Update the data which is used by the data table to display the
    // scopes/permissions every time the form field value has been changed.
    this.form.get(this.inputField).valueChanges.subscribe((value) => {
      const scopesPermissions: any[] = [];
      _.each(this.scopes, (scope) => {
        // Set the defaults values.
        const scopePermission: any = { read: false, write: false, execute: false };
        scopePermission['scope'] = scope;
        // Apply settings from the given value if they exist.
        if (scope in value) {
          _.each(value[scope], (permission) => {
            scopePermission[permission] = true;
          });
        }
        scopesPermissions.push(scopePermission);
      });
      this.data = scopesPermissions;
    });
  }

  /**
   * Checks if the specified row checkbox needs to be rendered as checked.
   * @param {string} scope The scope to be checked, e.g. 'cephfs', 'grafana',
   *   'osd', 'pool' ...
   * @return Returns true if all permissions (read, create, update, delete)
   *   are checked for the specified scope, otherwise false.
   */
  isRowChecked(scope: string) {
    const scope_permission = _.find(this.data, (o) => {
      return o['scope'] === scope;
    });
    if (_.isUndefined(scope_permission)) {
      return false;
    }
    if (this.isTableForOctalMode) {
      return scope_permission['read'] && scope_permission['write'] && scope_permission['execute'];
    }
    return (
      scope_permission['read'] &&
      scope_permission['create'] &&
      scope_permission['update'] &&
      scope_permission['delete']
    );
  }

  /**
   * Checks if the specified header checkbox needs to be rendered as checked.
   * @param {string} property The property/permission (read, create,
   *   update, delete) to be checked. If 'scope' is given, all permissions
   *   are checked.
   * @return Returns true if specified property/permission is selected
   *   for all scopes, otherwise false.
   */
  isHeaderChecked(property: string) {
    let permissions = [property];
    if ('scope' === property && this.isTableForOctalMode) {
      permissions = ['read', 'write', 'execute'];
    } else if ('scope' === property) {
      permissions = ['read', 'create', 'update', 'delete'];
    }
    return permissions.every((permission) => {
      return this.data.every((scope_permission) => {
        return scope_permission[permission];
      });
    });
  }

  onClickCellCheckbox(scope: string, property: string, event: any = null) {
    // Use a copy of the form field data to do not trigger the redrawing of the
    // data table with every change.
    const scopes_permissions = _.cloneDeep(this.form.getValue(this.inputField));
    let permissions = [property];
    if ('scope' === property && this.isTableForOctalMode) {
      permissions = ['read', 'write', 'execute'];
    } else if ('scope' === property) {
      permissions = ['read', 'create', 'update', 'delete'];
    }
    if (!(scope in scopes_permissions)) {
      scopes_permissions[scope] = [];
    }
    // Add or remove the given permission(s) depending on the click event or if no
    // click event is given then add/remove them if they are absent/exist.
    if (
      (event && event.target['checked']) ||
      !_.isEqual(permissions.sort(), _.intersection(scopes_permissions[scope], permissions).sort())
    ) {
      scopes_permissions[scope] = _.union(scopes_permissions[scope], permissions);
    } else {
      scopes_permissions[scope] = _.difference(scopes_permissions[scope], permissions);
      if (_.isEmpty(scopes_permissions[scope])) {
        _.unset(scopes_permissions, scope);
      }
    }
    this.form.get(this.inputField).setValue(scopes_permissions);
  }

  onClickHeaderCheckbox(property: string, event: any) {
    // Use a copy of the form field data to do not trigger the redrawing of the
    // data table with every change.
    const scopes_permissions = _.cloneDeep(this.form.getValue(this.inputField));
    let permissions = [property];
    if ('scope' === property && this.isTableForOctalMode) {
      permissions = ['read', 'write', 'execute'];
    } else if ('scope' === property) {
      permissions = ['read', 'create', 'update', 'delete'];
    }
    _.each(permissions, (permission) => {
      _.each(this.scopes, (scope) => {
        if (event.target['checked']) {
          scopes_permissions[scope] = _.union(scopes_permissions[scope], [permission]);
        } else {
          scopes_permissions[scope] = _.difference(scopes_permissions[scope], [permission]);
          if (_.isEmpty(scopes_permissions[scope])) {
            _.unset(scopes_permissions, scope);
          }
        }
      });
    });
    this.form.get(this.inputField).setValue(scopes_permissions);
  }
}
