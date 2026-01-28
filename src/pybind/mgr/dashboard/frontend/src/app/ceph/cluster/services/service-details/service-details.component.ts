import { Component, EventEmitter, Input, Output } from '@angular/core';

import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Permissions } from '~/app/shared/models/permissions';

@Component({
  selector: 'cd-service-details',
  templateUrl: './service-details.component.html',
  styleUrls: ['./service-details.component.scss'],
  standalone: false
})
export class ServiceDetailsComponent {
  @Input()
  permissions: Permissions;

  @Input()
  selection: CdTableSelection | any;

  @Output()
  editService = new EventEmitter<{ serviceName?: string; serviceType?: string }>();

  get service() {
    return this.selection as any;
  }

  get certificate() {
    return this.service?.certificate;
  }

  get hasCertificate() {
    const cert = this.certificate;
    return !!cert && cert.has_certificate;
  }

  onEditService(payload: { serviceName?: string; serviceType?: string }) {
    this.editService.emit(payload);
  }
}
