import { AfterViewInit, Directive, ElementRef, Optional } from '@angular/core';

import { Permissions } from '../models/permissions';
import { AuthStorageService } from '../services/auth-storage.service';
import { FormScopeDirective } from './form-scope.directive';

@Directive({
  selector:
    'input:not([cdNoFormInputDisable]), select:not([cdNoFormInputDisable]), [cdFormInputDisable]'
})
export class FormInputDisableDirective implements AfterViewInit {
  permissions: Permissions;
  service_name: keyof Permissions;

  constructor(
    @Optional() private formScope: FormScopeDirective,
    private authStorageService: AuthStorageService,
    private elementRef: ElementRef
  ) {}

  ngAfterViewInit() {
    this.permissions = this.authStorageService.getPermissions();
    if (this.formScope !== null) {
      this.service_name = this.formScope.cdFormScope;
    }
    if (this.service_name && !this.permissions[this.service_name].update) {
      this.elementRef.nativeElement.disabled = true;
    }
  }
}
