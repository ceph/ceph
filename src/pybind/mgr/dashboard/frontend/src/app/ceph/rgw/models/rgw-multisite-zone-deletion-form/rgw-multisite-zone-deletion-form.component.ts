import { Component, OnInit } from '@angular/core';
import { FormControl } from '@angular/forms';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { RgwZoneService } from '~/app/shared/api/rgw-zone.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { NotificationService } from '~/app/shared/services/notification.service';

@Component({
  selector: 'cd-rgw-multisite-zone-deletion-form',
  templateUrl: './rgw-multisite-zone-deletion-form.component.html',
  styleUrls: ['./rgw-multisite-zone-deletion-form.component.scss']
})
export class RgwMultisiteZoneDeletionFormComponent implements OnInit {
  zoneData$: any;
  zone: any;
  zoneForm: CdFormGroup;
  displayText: boolean = false;

  constructor(
    public activeModal: NgbActiveModal,
    public actionLabels: ActionLabelsI18n,
    public notificationService: NotificationService,
    private rgwZoneService: RgwZoneService
  ) {
    this.createForm();
  }

  ngOnInit(): void {
    this.zoneData$ = this.rgwZoneService.get(this.zone);
  }

  createForm() {
    this.zoneForm = new CdFormGroup({
      deletePools: new FormControl(false)
    });
  }

  submit() {
    this.rgwZoneService
      .delete(this.zone.parent, this.zone.name, this.zoneForm.value.deletePools)
      .subscribe(
        () => {
          this.notificationService.show(
            NotificationType.success,
            $localize`Zone: '${this.zone.name}' deleted successfully`
          );
          this.activeModal.close();
        },
        () => {
          this.zoneForm.setErrors({ cdSubmitButton: true });
        }
      );
  }

  showDangerText() {
    this.displayText = !this.displayText;
  }
}
