import { CommonModule } from '@angular/common';
import {
  Component,
  EventEmitter,
  Input,
  OnChanges,
  OnInit,
  Output,
  SimpleChanges
} from '@angular/core';
import { ReactiveFormsModule } from '@angular/forms';
import { CheckboxModule, GridModule, LayoutModule, RadioModule } from 'carbon-components-angular';

import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import {
  CephServiceCertificate,
  CertificateType,
  CertMode,
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
export class CertificateAuthorityFormComponent implements OnInit, OnChanges {
  readonly CertificateType = CertificateType;
  readonly CertMode = CertMode;
  readonly statusIconMap = CERTIFICATE_STATUS_ICON_MAP;

  @Input() formGroup: CdFormGroup;
  @Input() editing = false;
  @Input() currentCertificate: CephServiceCertificate = null;
  @Input() showCertSourceChangeWarning = false;
  @Input() certMode: CertMode = CertMode.both;

  @Output() certificateTypeChange = new EventEmitter<CertificateType>();

  ngOnInit(): void {
    this.setDefaultCertificateType();
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes['certMode'] && !changes['certMode'].firstChange) {
      this.setDefaultCertificateType();
    }
  }

  private setDefaultCertificateType(): void {
    if (!this.formGroup) return;
    const ctrl = this.formGroup.get('certificateType');
    if (!ctrl) return;

    if (this.certMode === CertMode.externalOnly) {
      ctrl.setValue(CertificateType.external, { emitEvent: false });
    } else if (this.certMode === CertMode.internalOnly) {
      ctrl.setValue(CertificateType.internal, { emitEvent: false });
    }
  }

  onCertificateTypeChange(type: CertificateType): void {
    this.certificateTypeChange.emit(type);
  }
}
