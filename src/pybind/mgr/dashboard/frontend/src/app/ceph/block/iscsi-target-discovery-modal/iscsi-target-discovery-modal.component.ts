import { Component, OnInit } from '@angular/core';
import { UntypedFormControl, Validators } from '@angular/forms';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

import { IscsiService } from '~/app/shared/api/iscsi.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { NotificationService } from '~/app/shared/services/notification.service';

@Component({
  selector: 'cd-iscsi-target-discovery-modal',
  templateUrl: './iscsi-target-discovery-modal.component.html',
  styleUrls: ['./iscsi-target-discovery-modal.component.scss']
})
export class IscsiTargetDiscoveryModalComponent implements OnInit {
  discoveryForm: CdFormGroup;
  permission: Permission;
  hasPermission: boolean;

  USER_REGEX = /^[\w\.:@_-]{8,64}$/;
  PASSWORD_REGEX = /^[\w@\-_\/]{12,16}$/;

  constructor(
    private authStorageService: AuthStorageService,
    public activeModal: NgbActiveModal,
    public actionLabels: ActionLabelsI18n,
    private iscsiService: IscsiService,
    private notificationService: NotificationService
  ) {
    this.permission = this.authStorageService.getPermissions().iscsi;
  }

  ngOnInit() {
    this.hasPermission = this.permission.update;
    this.createForm();
    this.iscsiService.getDiscovery().subscribe((auth) => {
      this.discoveryForm.patchValue(auth);
    });
  }

  createForm() {
    this.discoveryForm = new CdFormGroup({
      user: new UntypedFormControl({ value: '', disabled: !this.hasPermission }),
      password: new UntypedFormControl({ value: '', disabled: !this.hasPermission }),
      mutual_user: new UntypedFormControl({ value: '', disabled: !this.hasPermission }),
      mutual_password: new UntypedFormControl({ value: '', disabled: !this.hasPermission })
    });

    CdValidators.validateIf(
      this.discoveryForm.get('user'),
      () =>
        this.discoveryForm.getValue('password') ||
        this.discoveryForm.getValue('mutual_user') ||
        this.discoveryForm.getValue('mutual_password'),
      [Validators.required],
      [Validators.pattern(this.USER_REGEX)],
      [
        this.discoveryForm.get('password'),
        this.discoveryForm.get('mutual_user'),
        this.discoveryForm.get('mutual_password')
      ]
    );

    CdValidators.validateIf(
      this.discoveryForm.get('password'),
      () =>
        this.discoveryForm.getValue('user') ||
        this.discoveryForm.getValue('mutual_user') ||
        this.discoveryForm.getValue('mutual_password'),
      [Validators.required],
      [Validators.pattern(this.PASSWORD_REGEX)],
      [
        this.discoveryForm.get('user'),
        this.discoveryForm.get('mutual_user'),
        this.discoveryForm.get('mutual_password')
      ]
    );

    CdValidators.validateIf(
      this.discoveryForm.get('mutual_user'),
      () => this.discoveryForm.getValue('mutual_password'),
      [Validators.required],
      [Validators.pattern(this.USER_REGEX)],
      [
        this.discoveryForm.get('user'),
        this.discoveryForm.get('password'),
        this.discoveryForm.get('mutual_password')
      ]
    );

    CdValidators.validateIf(
      this.discoveryForm.get('mutual_password'),
      () => this.discoveryForm.getValue('mutual_user'),
      [Validators.required],
      [Validators.pattern(this.PASSWORD_REGEX)],
      [
        this.discoveryForm.get('user'),
        this.discoveryForm.get('password'),
        this.discoveryForm.get('mutual_user')
      ]
    );
  }

  submitAction() {
    this.iscsiService.updateDiscovery(this.discoveryForm.value).subscribe(
      () => {
        this.notificationService.show(
          NotificationType.success,
          $localize`Updated discovery authentication`
        );
        this.activeModal.close();
      },
      () => {
        this.discoveryForm.setErrors({ cdSubmitButton: true });
      }
    );
  }
}
