import { Component, OnInit } from '@angular/core';
import { FormControl } from '@angular/forms';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { RgwZonegroupService } from '~/app/shared/api/rgw-zonegroup.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { NotificationService } from '~/app/shared/services/notification.service';

@Component({
  selector: 'cd-rgw-multisite-zonegroup-deletion-form',
  templateUrl: './rgw-multisite-zonegroup-deletion-form.component.html',
  styleUrls: ['./rgw-multisite-zonegroup-deletion-form.component.scss']
})
export class RgwMultisiteZonegroupDeletionFormComponent implements OnInit {
  zonegroupData$: any;
  zonegroup: any;
  zonegroupForm: CdFormGroup;
  displayText: boolean = false;

  constructor(
    public activeModal: NgbActiveModal,
    public actionLabels: ActionLabelsI18n,
    public notificationService: NotificationService,
    private rgwZonegroupService: RgwZonegroupService
  ) {
    this.createForm();
  }

  ngOnInit(): void {
    this.zonegroupData$ = this.rgwZonegroupService.get(this.zonegroup);
  }

  createForm() {
    this.zonegroupForm = new CdFormGroup({
      deletePools: new FormControl(false)
    });
  }

  submit() {
    this.rgwZonegroupService
      .delete(this.zonegroup.name, this.zonegroupForm.value.deletePools)
      .subscribe(() => {
        this.notificationService.show(
          NotificationType.success,
          $localize`Zone: '${this.zonegroup.name}' deleted successfully`
        );
        this.activeModal.close();
      });
  }

  showDangerText() {
    this.displayText = !this.displayText;
  }
}
