import { Component, OnInit } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';
import { Observable } from 'rxjs';

import { Icons } from '~/app/shared/enum/icons.enum';
import { Permission } from '~/app/shared/models/permissions';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { UpgradeService } from '~/app/shared/api/upgrade.service';
import { UpgradeInfoInterface } from '~/app/shared/models/upgrade.interface';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { NotificationService } from '~/app/shared/services/notification.service';
import { LicenceAgreementComponent } from '../../license-agreement/license-agreement.component';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';

@Component({
  selector: 'cd-upgrade-start-modal.component',
  templateUrl: './upgrade-start-modal.component.html',
  styleUrls: ['./upgrade-start-modal.component.scss'],
  standalone: false
})
export class UpgradeStartModalComponent implements OnInit {
  permission: Permission;
  upgradeInfoError$: Observable<any>;
  upgradeInfo$: Observable<UpgradeInfoInterface>;
  upgradeForm: CdFormGroup;
  icons = Icons;
  versions: string[];

  showImageField = false;

  constructor(
    public actionLabels: ActionLabelsI18n,
    private authStorageService: AuthStorageService,
    public activeModal: NgbActiveModal,
    private upgradeService: UpgradeService,
    private notificationService: NotificationService,
    private modalCdsService: ModalCdsService
  ) {
    this.permission = this.authStorageService.getPermissions().configOpt;
  }

  ngOnInit() {
    this.upgradeForm = new CdFormGroup({
      availableVersions: new FormControl(null, [Validators.required]),
      useImage: new FormControl(false),
      customImageName: new FormControl(null)
    });
    if (this.versions === undefined) {
      const availableVersionsControl = this.upgradeForm.get('availableVersions');
      availableVersionsControl.clearValidators();
      const customImageNameControl = this.upgradeForm.get('customImageName');
      customImageNameControl.setValidators(Validators.required);
      customImageNameControl.updateValueAndValidity();
    }
  }

  showLicenceAgreement() {
    const version = this.upgradeForm.getValue('availableVersions');
    const useImage = this.upgradeForm.getValue('useImage');

    // If using custom image, skip license agreement and start upgrade directly
    if (useImage) {
      this.startUpgrade();
      return;
    }

    const [major, minor] = version.split('.').map(Number);

    // Show licence agreement modal if the version is 9.1 or above
    if (major > 9 || (major === 9 && minor >= 1)) {
      const modalRef = this.modalCdsService.show(LicenceAgreementComponent, {
        manifest: version
      });
      modalRef.acceptanceEvent.subscribe((accepted: boolean) => {
        if (accepted) {
          this.startUpgrade();
        }
      });
    } else {
      this.startUpgrade();
    }
  }

  startUpgrade() {
    const version = this.upgradeForm.getValue('availableVersions');
    const image = this.upgradeForm.getValue('customImageName');
    this.upgradeService.start(version, image).subscribe({
      next: () => {
        this.notificationService.show(
          NotificationType.success,
          $localize`Started upgrading the cluster`
        );
      },
      error: (error) => {
        this.upgradeForm.setErrors({ cdSubmitButton: true });
        this.notificationService.show(
          NotificationType.error,
          $localize`Failed to start the upgrade`,
          error
        );
      },
      complete: () => {
        this.activeModal.close();
      }
    });
  }

  useImage() {
    this.showImageField = !this.showImageField;
    const availableVersionsControl = this.upgradeForm.get('availableVersions');
    const customImageNameControl = this.upgradeForm.get('customImageName');

    if (this.showImageField) {
      availableVersionsControl.disable();
      availableVersionsControl.clearValidators();

      customImageNameControl.setValidators(Validators.required);
      customImageNameControl.updateValueAndValidity();
    } else {
      availableVersionsControl.enable();
      availableVersionsControl.setValidators(Validators.required);
      availableVersionsControl.updateValueAndValidity();

      customImageNameControl.clearValidators();
    }
  }
}
