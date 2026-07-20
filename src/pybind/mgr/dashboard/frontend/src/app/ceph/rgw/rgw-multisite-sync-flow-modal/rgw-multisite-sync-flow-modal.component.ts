import { Component, Inject, OnInit } from '@angular/core';
import { UntypedFormControl, Validators } from '@angular/forms';
import { RgwDaemonService } from '~/app/shared/api/rgw-daemon.service';
import { ActionLabelsI18n, SucceededActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { NotificationService } from '~/app/shared/services/notification.service';
import { catchError, switchMap } from 'rxjs/operators';
import { RgwZonegroupService } from '~/app/shared/api/rgw-zonegroup.service';
import { RgwDaemon } from '../models/rgw-daemon';
import { FlowType, RgwZonegroup, Zone } from '../models/rgw-multisite';
import { of } from 'rxjs';
import { SelectOption } from '~/app/shared/components/select/select-option.model';
import _ from 'lodash';
import { Icons } from '~/app/shared/enum/icons.enum';
import { RgwMultisiteService } from '~/app/shared/api/rgw-multisite.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { BaseModal } from 'carbon-components-angular';
import { ComboBoxItem } from '~/app/shared/models/combo-box.model';

@Component({
  selector: 'cd-rgw-multisite-sync-flow-modal',
  templateUrl: './rgw-multisite-sync-flow-modal.component.html',
  styleUrls: ['./rgw-multisite-sync-flow-modal.component.scss']
})
export class RgwMultisiteSyncFlowModalComponent extends BaseModal implements OnInit {
  editing: boolean = false;
  syncPolicyDirectionalFlowForm: CdFormGroup;
  syncPolicySymmetricalFlowForm: CdFormGroup;
  syncPolicyPipeForm: CdFormGroup;
  currentFormGroupContext: CdFormGroup;
  flowType = FlowType;
  icons = Icons;
  zones: ComboBoxItem[] = [];

  constructor(
    @Inject('groupType') public groupType: FlowType,
    @Inject('groupExpandedRow') public groupExpandedRow: { groupName: string; bucket: string },
    @Inject('flowSelectedRow') public flowSelectedRow: { id: string; zones: string[] },
    @Inject('action') public action: string,
    public actionLabels: ActionLabelsI18n,
    public notificationService: NotificationService,
    private rgwDaemonService: RgwDaemonService,
    private rgwZonegroupService: RgwZonegroupService,
    private rgwMultisiteService: RgwMultisiteService,
    private succeededLabels: SucceededActionLabelsI18n
  ) {
    super();
  }

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
    if (this.editing) {
      this.currentFormGroupContext.patchValue({
        flow_id: this.flowSelectedRow.id,
        bucket_name: this.groupExpandedRow.bucket || ''
      });
      this.currentFormGroupContext.get('flow_id').disable();
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
      .subscribe((zonegroupData: { zones: { name: string }[] }) => {
        if (zonegroupData && zonegroupData?.zones?.length > 0) {
          const zones: SelectOption[] = [];
          zonegroupData.zones.forEach((zone: { name: string }) => {
            zones.push(new SelectOption(false, zone.name, ''));
          });
          this.zones = [...zones].map((zone: { name: string }) => {
            return { name: zone.name, content: zone.name };
          });
          if (this.editing) {
            // @TODO: Editing/deletion of directional flow not supported yet.
            // Integrate it once the backend supports it.
            if (this.groupType === FlowType.symmetrical) {
              this.zones = [...zones].map((zone: { name: string }) => {
                if (this.flowSelectedRow.zones.includes(zone.name)) {
                  return { name: zone.name, content: zone.name, selected: true };
                }
                return { name: zone.name, content: zone.name };
              });
              this.currentFormGroupContext.patchValue({ zones: this.zones });
            }
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

  getZoneData(zoneDataToFilter: string[], zoneDataForCondition: string[]) {
    return zoneDataToFilter.filter((zone: string) => !zoneDataForCondition.includes(zone));
  }

  assignZoneValue(zone: string[], selectedZone: string[]) {
    return zone.length > 0 ? zone : selectedZone;
  }

  submit() {
    const zones: Zone = { added: [], removed: [] };

    if (this.currentFormGroupContext.invalid) {
      return;
    }
    // Ensure that no validation is pending
    if (this.currentFormGroupContext.pending) {
      this.currentFormGroupContext.setErrors({ cdSubmitButton: true });
      return;
    }

    if (this.groupType == FlowType.symmetrical) {
      const selectedZones = this.currentFormGroupContext.get('zones').value;
      if (this.editing) {
        zones.removed = this.getZoneData(this.flowSelectedRow.zones, selectedZones);
        zones.added = this.getZoneData(selectedZones, this.flowSelectedRow.zones);
      }
      zones.added = this.assignZoneValue(zones.added, selectedZones);
    }
    this.rgwMultisiteService
      .createEditSyncFlow({ ...this.currentFormGroupContext.getRawValue(), zones: zones })
      .subscribe({
        next: () => {
          const action = this.editing ? this.succeededLabels.EDITED : this.succeededLabels.CREATED;
          this.notificationService.show(
            NotificationType.success,
            $localize`${action} Sync Flow '${this.currentFormGroupContext.getValue('flow_id')}'`
          );
        },
        error: () => {
          // Reset the 'Submit' button.
          this.currentFormGroupContext.setErrors({ cdSubmitButton: true });
        },
        complete: () => {
          this.closeModal();
        }
      });
  }
}
