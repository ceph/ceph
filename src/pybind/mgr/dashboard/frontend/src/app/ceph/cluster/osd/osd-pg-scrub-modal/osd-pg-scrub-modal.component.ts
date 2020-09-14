import { Component, ViewChild } from '@angular/core';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { forkJoin as observableForkJoin } from 'rxjs';

import { ConfigOptionComponent } from '../../../../shared/components/config-option/config-option.component';
import { ActionLabelsI18n } from '../../../../shared/constants/app.constants';
import { NotificationType } from '../../../../shared/enum/notification-type.enum';
import { CdFormGroup } from '../../../../shared/forms/cd-form-group';
import { Permissions } from '../../../../shared/models/permissions';
import { AuthStorageService } from '../../../../shared/services/auth-storage.service';
import { NotificationService } from '../../../../shared/services/notification.service';
import { OsdPgScrubModalOptions } from './osd-pg-scrub-modal.options';

@Component({
  selector: 'cd-osd-pg-scrub-modal',
  templateUrl: './osd-pg-scrub-modal.component.html',
  styleUrls: ['./osd-pg-scrub-modal.component.scss']
})
export class OsdPgScrubModalComponent {
  osdPgScrubForm: CdFormGroup;
  action: string;
  resource: string;
  permissions: Permissions;

  @ViewChild('basicOptionsValues', { static: true })
  basicOptionsValues: ConfigOptionComponent;
  basicOptions: Array<string> = OsdPgScrubModalOptions.basicOptions;

  @ViewChild('advancedOptionsValues')
  advancedOptionsValues: ConfigOptionComponent;
  advancedOptions: Array<string> = OsdPgScrubModalOptions.advancedOptions;

  advancedEnabled = false;

  constructor(
    public activeModal: NgbActiveModal,
    private authStorageService: AuthStorageService,
    private notificationService: NotificationService,
    public actionLabels: ActionLabelsI18n
  ) {
    this.osdPgScrubForm = new CdFormGroup({});
    this.resource = $localize`PG scrub options`;
    this.action = this.actionLabels.EDIT;
    this.permissions = this.authStorageService.getPermissions();
  }

  submitAction() {
    const observables = [this.basicOptionsValues.saveValues()];

    if (this.advancedOptionsValues) {
      observables.push(this.advancedOptionsValues.saveValues());
    }

    observableForkJoin(observables).subscribe(
      () => {
        this.notificationService.show(
          NotificationType.success,
          $localize`Updated PG scrub options`
        );
        this.activeModal.close();
      },
      () => {
        this.activeModal.close();
      }
    );
  }
}
