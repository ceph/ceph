import { Directive, Input, TemplateRef, ViewContainerRef } from '@angular/core';

import _ from 'lodash';

import { Permission, Permissions } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';

type Condition = string | string[] | Partial<{ [Property in keyof Permissions]: keyof Permission }>;

@Directive({
  selector: '[cdScope]'
})
export class AuthStorageDirective {
  permissions: Permissions;

  constructor(
    private templateRef: TemplateRef<any>,
    private viewContainer: ViewContainerRef,
    private authStorageService: AuthStorageService
  ) {}

  @Input() set cdScope(condition: Condition) {
    this.permissions = this.authStorageService.getPermissions();
    if (this.isAuthorized(condition)) {
      this.viewContainer.createEmbeddedView(this.templateRef);
    } else {
      this.viewContainer.clear();
    }
  }

  @Input() cdScopeMatchAll = true;

  private isAuthorized(condition: Condition): boolean {
    const everyOrSome = this.cdScopeMatchAll ? _.every : _.some;

    if (_.isString(condition)) {
      return _.get(this.permissions, [condition, 'read'], false);
    } else if (_.isArray(condition)) {
      return everyOrSome(condition, (permission) => this.permissions[permission]['read']);
    } else if (_.isObject(condition)) {
      return everyOrSome(condition, (value, key) => {
        return everyOrSome(value, (val) => this.permissions[key][val]);
      });
    }

    return false;
  }
}
