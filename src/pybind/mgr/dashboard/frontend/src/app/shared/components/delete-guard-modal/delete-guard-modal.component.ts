import { Component, Inject, Optional, TemplateRef } from '@angular/core';
import { Router } from '@angular/router';
import { BaseModal } from 'carbon-components-angular';
import { ConnectedItem } from '../../models/delete-guard.model';

@Component({
  selector: 'cd-delete-guard-modal',
  templateUrl: './delete-guard-modal.component.html',
  standalone: false
})
export class DeleteGuardModalComponent extends BaseModal {
  constructor(
    private router: Router,
    @Optional() @Inject('resourceName') public resourceName: string,
    @Optional() @Inject('resourceType') public resourceType: string,
    @Optional() @Inject('connectedItems') public connectedItems: ConnectedItem[],
    @Optional() @Inject('message') public message: string,
    @Optional() @Inject('connectedItemsLabel') public connectedItemsLabel: string,
    @Optional() @Inject('bodyTemplate') public bodyTemplate: TemplateRef<any>,
    @Optional() @Inject('bodyContext') public bodyContext: any
  ) {
    super();
    this.resourceType = this.resourceType || $localize`resource`;
    this.connectedItems = this.connectedItems || [];
    this.message =
      this.message ||
      $localize`This resource has connected items that must be deleted first. Delete the connected items, and try again.`;
    this.connectedItemsLabel = this.connectedItemsLabel || $localize`View connected items:`;
  }

  navigateToItem(item: ConnectedItem): void {
    if (item.route?.length) {
      this.router.navigate(item.route, {
        queryParams: item.queryParams
      });
      this.closeModal();
    }
  }
}
