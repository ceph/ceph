import { AfterViewInit, Directive, ElementRef, Optional } from '@angular/core';

import { Permissions } from '../models/permissions';
import { AuthStorageService } from '../services/auth-storage.service';
import { FormScopeDirective } from './form-scope.directive';

@Directive({
  selector:
    'input:not([cdNoFormInputDisable]), select:not([cdNoFormInputDisable]), button:not([cdNoFormInputDisable]), [cdFormInputDisable]'
})
export class FormInputDisableDirective implements AfterViewInit {
  permissions: Permissions;

  constructor(
    @Optional() private formScope: FormScopeDirective,
    private authStorageService: AuthStorageService,
    private elementRef: ElementRef
  ) {}

  ngAfterViewInit() {
    this.permissions = this.authStorageService.getPermissions();
    const service_name = this.formScope?.cdFormScope;
    if (service_name && !this.permissions?.[service_name]?.update) {
      this.elementRef.nativeElement.disabled = true;
    }
  }
}
