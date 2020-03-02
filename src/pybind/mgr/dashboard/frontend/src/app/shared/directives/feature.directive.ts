import { Directive, EmbeddedViewRef, Input, TemplateRef, ViewContainerRef } from '@angular/core';

import { Permissions } from '../models/permissions';
import { AuthStorageService } from '../services/auth-storage.service';

@Directive({
  selector: '[cdFeature]'
})
export class FeatureDirective {
  private permissions: Permissions;
  private view: EmbeddedViewRef<any>;

  @Input() set cdFeature(feature_name: keyof Permissions) {
    if (feature_name === undefined || this.permissions[feature_name].read) {
      if (this.view === undefined) {
        this.view = this.viewContainer.createEmbeddedView(this.templateRef);
      }
    } else if (this.view !== undefined) {
      this.viewContainer.clear();
      this.view = undefined;
    }
  }

  constructor(
    private templateRef: TemplateRef<any>,
    private viewContainer: ViewContainerRef,
    private authStorageService: AuthStorageService
  ) {
    this.permissions = this.authStorageService.getPermissions();
  }
}
