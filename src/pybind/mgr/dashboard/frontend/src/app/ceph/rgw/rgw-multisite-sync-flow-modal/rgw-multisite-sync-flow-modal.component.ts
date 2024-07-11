import { Component, OnInit } from '@angular/core';
import { UntypedFormControl, Validators } from '@angular/forms';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { RgwDaemonService } from '~/app/shared/api/rgw-daemon.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { NotificationService } from '~/app/shared/services/notification.service';
import { catchError, switchMap } from 'rxjs/operators';
import { RgwZonegroupService } from '~/app/shared/api/rgw-zonegroup.service';
import { RgwDaemon } from '../models/rgw-daemon';
import { FlowType, RgwZonegroup } from '../models/rgw-multisite';
import { of } from 'rxjs';
import { SelectOption } from '~/app/shared/components/select/select-option.model';
import _ from 'lodash';
import { Icons } from '~/app/shared/enum/icons.enum';
import { RgwMultisiteService } from '~/app/shared/api/rgw-multisite.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { ZoneData } from '../models/rgw-multisite-zone-selector';

@Component({
  selector: 'cd-rgw-multisite-sync-flow-modal',
  templateUrl: './rgw-multisite-sync-flow-modal.component.html',
  styleUrls: ['./rgw-multisite-sync-flow-modal.component.scss']
})
export class RgwMultisiteSyncFlowModalComponent implements OnInit {
  action: string;
  editing: boolean = false;
  groupType: FlowType;
  groupExpandedRow: any;
  flowSelectedRow: any;
  syncPolicyDirectionalFlowForm: CdFormGroup;
  syncPolicySymmetricalFlowForm: CdFormGroup;
  syncPolicyPipeForm: CdFormGroup;
  currentFormGroupContext: CdFormGroup;
  flowType = FlowType;
  icons = Icons;
  zones = new ZoneData(false, 'Filter Zones');
  sourceZones = new ZoneData(false, 'Filter Zones');
  destinationZones = new ZoneData(true, 'Filter or Add Zones');

  constructor(
    public activeModal: NgbActiveModal,
    public actionLabels: ActionLabelsI18n,
    public notificationService: NotificationService,
    private rgwDaemonService: RgwDaemonService,
    private rgwZonegroupService: RgwZonegroupService,
    private rgwMultisiteService: RgwMultisiteService
  ) {}

  ngOnInit(): void {
    if (this.action === 'edit') {
      this.editing = true;
    }
    if (this.groupType === FlowType.symmetrical) {
      this.createSymmetricalFlowForm();
      this.currentFormGroupContext = _.cloneDeep(this.syncPolicySymmetricalFlowForm);
    } else if (this.groupType === FlowType.directional) {
      this.createDirectionalFlowForm();
      this.currentFormGroupContext = _.cloneDeep(this.syncPolicyDirectionalFlowForm);
    }
    this.currentFormGroupContext.get('bucket_name').disable();
    if (this.editing) {
      this.currentFormGroupContext.patchValue({
        flow_id: this.flowSelectedRow.id,
        bucket_name: this.groupExpandedRow.bucket || ''
      });
    }

    this.rgwDaemonService.selectedDaemon$
      .pipe(
        switchMap((daemon: RgwDaemon) => {
          if (daemon) {
            const zonegroupObj = new RgwZonegroup();
            zonegroupObj.name = daemon?.zonegroup_name;
            return this.rgwZonegroupService.get(zonegroupObj).pipe(
              catchError(() => {
                return of([]);
              })
            );
          } else {
            return of([]);
          }
        })
      )
      .subscribe((zonegroupData: any) => {
        if (zonegroupData && zonegroupData?.zones?.length > 0) {
          const zones: any = [];
          zonegroupData.zones.forEach((zone: any) => {
            zones.push(new SelectOption(false, zone.name, ''));
          });
          this.zones.data.available = [...zones];
          this.sourceZones.data.available = [...zones];
          if (this.editing) {
            if (this.groupType === FlowType.symmetrical) {
              this.zones.data.selected = this.flowSelectedRow.zones;
            } else {
              this.destinationZones.data.selected = [this.flowSelectedRow.dest_zone];
              this.sourceZones.data.selected = [this.flowSelectedRow.source_zone];
            }
            this.zoneSelection();
          }
        }
      });
  }

  createSymmetricalFlowForm() {
    this.syncPolicySymmetricalFlowForm = new CdFormGroup({
      ...this.commonFormControls(FlowType.symmetrical),
      zones: new UntypedFormControl([], {
        validators: [Validators.required]
      })
    });
  }

  createDirectionalFlowForm() {
    this.syncPolicyDirectionalFlowForm = new CdFormGroup({
      ...this.commonFormControls(FlowType.directional),
      source_zone: new UntypedFormControl('', {
        validators: [Validators.required]
      }),
      destination_zone: new UntypedFormControl('', {
        validators: [Validators.required]
      })
    });
  }

  commonFormControls(flowType: FlowType) {
    return {
      bucket_name: new UntypedFormControl(this.groupExpandedRow?.bucket),
      group_id: new UntypedFormControl(this.groupExpandedRow?.groupName, {
        validators: [Validators.required]
      }),
      flow_id: new UntypedFormControl('', {
        validators: [Validators.required]
      }),
      flow_type: new UntypedFormControl(flowType, {
        validators: [Validators.required]
      })
    };
  }

  zoneSelection() {
    if (this.groupType === FlowType.symmetrical) {
      this.currentFormGroupContext.patchValue({
        zones: this.zones.data.selected
      });
    } else {
      this.currentFormGroupContext.patchValue({
        source_zone: this.sourceZones.data.selected,
        destination_zone: this.destinationZones.data.selected
      });
    }
  }

  submit() {
    if (this.currentFormGroupContext.invalid) {
      return;
    }
    // Ensure that no validation is pending
    if (this.currentFormGroupContext.pending) {
      this.currentFormGroupContext.setErrors({ cdSubmitButton: true });
      return;
    }
    this.rgwMultisiteService
      .createEditSyncFlow(this.currentFormGroupContext.getRawValue())
      .subscribe(
        () => {
          const action = this.editing ? 'Modified' : 'Created';
          this.notificationService.show(
            NotificationType.success,
            $localize`${action} Sync Flow '${this.currentFormGroupContext.getValue('flow_id')}'`
          );
          this.activeModal.close('success');
        },
        () => {
          // Reset the 'Submit' button.
          this.currentFormGroupContext.setErrors({ cdSubmitButton: true });
          this.activeModal.dismiss();
        }
      );
  }
}
