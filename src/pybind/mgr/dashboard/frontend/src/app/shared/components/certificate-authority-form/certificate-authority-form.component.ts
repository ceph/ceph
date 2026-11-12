import { CommonModule } from '@angular/common';
import { Component, EventEmitter, Input, Output } from '@angular/core';
import { ReactiveFormsModule } from '@angular/forms';
import { CheckboxModule, GridModule, LayoutModule, RadioModule } from 'carbon-components-angular';

import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import {
  CephServiceCertificate,
  CertificateType,
  CERTIFICATE_STATUS_ICON_MAP
} from '~/app/shared/models/service.interface';
import { PipesModule } from '~/app/shared/pipes/pipes.module';
import { ComponentsModule } from '../components.module';
import { TextLabelListComponent } from '../text-label-list/text-label-list.component';

@Component({
  selector: 'cd-certificate-authority-form',
  templateUrl: './certificate-authority-form.component.html',
  styleUrls: ['./certificate-authority-form.component.scss'],
  standalone: true,
  imports: [
    CommonModule,
    ReactiveFormsModule,
    RadioModule,
    CheckboxModule,
    GridModule,
    LayoutModule,
    PipesModule,
    ComponentsModule,
    TextLabelListComponent
  ]
})
export class CertificateAuthorityFormComponent {
  readonly CertificateType = CertificateType;
  readonly statusIconMap = CERTIFICATE_STATUS_ICON_MAP;

  @Input() formGroup: CdFormGroup;
  @Input() editing = false;
  @Input() currentCertificate: CephServiceCertificate = null;
  @Input() showCertSourceChangeWarning = false;

  @Output() certificateTypeChange = new EventEmitter<CertificateType>();

  onCertificateTypeChange(type: CertificateType): void {
    this.certificateTypeChange.emit(type);
  }
}
