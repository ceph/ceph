import { Component, OnInit } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';

import { I18n } from '@ngx-translate/i18n-polyfill';
import { BsModalRef } from 'ngx-bootstrap/modal';

import { IscsiService } from '../../../shared/api/iscsi.service';
import { NotificationType } from '../../../shared/enum/notification-type.enum';
import { CdFormGroup } from '../../../shared/forms/cd-form-group';
import { CdValidators } from '../../../shared/forms/cd-validators';
import { NotificationService } from '../../../shared/services/notification.service';

@Component({
  selector: 'cd-iscsi-target-discovery-modal',
  templateUrl: './iscsi-target-discovery-modal.component.html',
  styleUrls: ['./iscsi-target-discovery-modal.component.scss']
})
export class IscsiTargetDiscoveryModalComponent implements OnInit {
  discoveryForm: CdFormGroup;

  USER_REGEX = /[\w\.:@_-]{8,64}/;
  PASSWORD_REGEX = /[\w@\-_\/]{12,16}/;

  constructor(
    public bsModalRef: BsModalRef,
    private iscsiService: IscsiService,
    private notificationService: NotificationService,
    private i18n: I18n
  ) {
    this.discoveryForm = new CdFormGroup({
      user: new FormControl(''),
      password: new FormControl(''),
      mutual_user: new FormControl(''),
      mutual_password: new FormControl('')
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

  ngOnInit() {
    this.iscsiService.getDiscovery().subscribe((auth) => {
      this.discoveryForm.patchValue(auth);
    });
  }

  submitAction() {
    this.iscsiService.updateDiscovery(this.discoveryForm.value).subscribe(
      () => {
        this.notificationService.show(
          NotificationType.success,
          this.i18n('Updated discovery authentication')
        );
        this.bsModalRef.hide();
      },
      () => {
        this.bsModalRef.hide();
      }
    );
  }
}
