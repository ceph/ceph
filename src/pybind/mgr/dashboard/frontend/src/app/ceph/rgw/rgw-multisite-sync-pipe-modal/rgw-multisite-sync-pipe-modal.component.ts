import { Component, Inject, OnInit } from '@angular/core';
import { UntypedFormControl, Validators } from '@angular/forms';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { RgwZonegroup, Zone } from '../models/rgw-multisite';
import { SelectOption } from '~/app/shared/components/select/select-option.model';
import { catchError, switchMap } from 'rxjs/operators';
import { of } from 'rxjs';
import { RgwDaemon } from '../models/rgw-daemon';
import { RgwDaemonService } from '~/app/shared/api/rgw-daemon.service';
import { RgwZonegroupService } from '~/app/shared/api/rgw-zonegroup.service';
import _ from 'lodash';
import { Icons } from '~/app/shared/enum/icons.enum';
import { RgwMultisiteService } from '~/app/shared/api/rgw-multisite.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { NotificationService } from '~/app/shared/services/notification.service';
import { SucceededActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { BaseModal } from 'carbon-components-angular';
import { ComboBoxItem } from '~/app/shared/models/combo-box.model';

const ALL_ZONES = $localize`All zones (*)`;
const ALL_BUCKET_SELECTED_HELP_TEXT =
  'If no value is provided, all the buckets in the zone group will be selected.';

interface PipeParams {
  source: { zones: string[]; bucket: string };
  dest: { zones: string[]; bucket: string };
  id: string;
  params: { user: string; mode: string };
}

interface GroupParams {
  groupName: string;
  bucket: string;
}

@Component({
  selector: 'cd-rgw-multisite-sync-pipe-modal',
  templateUrl: './rgw-multisite-sync-pipe-modal.component.html',
  styleUrls: ['./rgw-multisite-sync-pipe-modal.component.scss']
})
export class RgwMultisiteSyncPipeModalComponent extends BaseModal implements OnInit {
  pipeForm: CdFormGroup;
  editing: boolean;
  sourceZones: ComboBoxItem[] = [];
  destZones: ComboBoxItem[] = [];
  icons = Icons;
  allBucketSelectedHelpText = ALL_BUCKET_SELECTED_HELP_TEXT;

  constructor(
    @Inject('groupExpandedRow') public groupExpandedRow: GroupParams,
    @Inject('pipeSelectedRow') public pipeSelectedRow: PipeParams,
    @Inject('action') public action: string,
    private rgwDaemonService: RgwDaemonService,
    private rgwZonegroupService: RgwZonegroupService,
    private rgwMultisiteService: RgwMultisiteService,
    private notificationService: NotificationService,
    private succeededLabels: SucceededActionLabelsI18n
  ) {
    super();
  }

  ngOnInit(): void {
    if (this.pipeSelectedRow) {
      this.pipeSelectedRow.source.zones = this.replaceAsteriskWithString(
        this.pipeSelectedRow.source.zones
      );
      this.pipeSelectedRow.dest.zones = this.replaceAsteriskWithString(
        this.pipeSelectedRow.dest.zones
      );
    }
    this.editing = this.action === 'create' ? false : true;
    this.pipeForm = new CdFormGroup({
      pipe_id: new UntypedFormControl('', {
        validators: [Validators.required]
      }),
      group_id: new UntypedFormControl(this.groupExpandedRow?.groupName || '', {
        validators: [Validators.required]
      }),
      bucket_name: new UntypedFormControl(this.groupExpandedRow?.bucket || ''),
      source_bucket: new UntypedFormControl(''),
      source_zones: new UntypedFormControl('', {
        validators: [Validators.required]
      }),
      destination_bucket: new UntypedFormControl(''),
      destination_zones: new UntypedFormControl('', {
        validators: [Validators.required]
      })
    });
    this.rgwDaemonService.selectedDaemon$
      .pipe(
        switchMap((daemon: RgwDaemon) => {
          if (daemon) {
            const zonegroupObj = new RgwZonegroup();
            zonegroupObj.name = daemon.zonegroup_name;
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
          let zones: SelectOption[] = [];
          zones.push(new SelectOption(false, ALL_ZONES, ''));
          zonegroupData.zones.forEach((zone: { name: string }) => {
            zones.push(new SelectOption(false, zone.name, ''));
          });
          this.sourceZones = JSON.parse(JSON.stringify(zones)).map((zone: { name: string }) => {
            return { name: zone.name, content: zone.name };
          });
          this.destZones = JSON.parse(JSON.stringify(zones)).map((zone: { name: string }) => {
            return { name: zone.name, content: zone.name };
          });
          if (this.editing) {
            this.pipeForm.get('pipe_id').disable();
            this.sourceZones = [...this.sourceZones].map((zone: { name: string }) => {
              if (this.pipeSelectedRow.source.zones.includes(zone.name)) {
                return { name: zone.name, content: zone.name, selected: true };
              }
              return { name: zone.name, content: zone.name };
            });
            this.destZones = [...this.destZones].map((zone: { name: string }) => {
              if (this.pipeSelectedRow.dest.zones.includes(zone.name)) {
                return { name: zone.name, content: zone.name, selected: true };
              }
              return { name: zone.name, content: zone.name };
            });
            const availableDestZone: SelectOption[] = [];
            this.pipeSelectedRow.dest.zones.forEach((zone: string) => {
              availableDestZone.push(new SelectOption(true, zone, ''));
            });
            this.pipeForm.patchValue({
              pipe_id: this.pipeSelectedRow.id,
              source_zones: this.pipeSelectedRow.source.zones,
              destination_zones: this.pipeSelectedRow.dest.zones,
              source_bucket: this.pipeSelectedRow.source.bucket,
              destination_bucket: this.pipeSelectedRow.dest.bucket
            });
          }
        }
      });
  }

  replaceWithAsterisk(zones: string[]) {
    return zones.map((str) => str.replace(ALL_ZONES, '*'));
  }

  replaceAsteriskWithString(zones: string[]) {
    return zones.map((str) => str.replace('*', ALL_ZONES));
  }

  getZoneData(zoneDataToFilter: string[], zoneDataForCondition: string[]) {
    return zoneDataToFilter.filter((zone: string) => !zoneDataForCondition.includes(zone));
  }

  assignZoneValue(zone: string[], selectedZone: string[]) {
    return zone.length > 0
      ? this.replaceWithAsterisk(zone)
      : this.replaceWithAsterisk(selectedZone);
  }

  submit() {
    const sourceZones: Zone = { added: [], removed: [] };
    const destZones: Zone = { added: [], removed: [] };

    if (this.pipeForm.invalid) {
      return;
    }
    // Ensure that no validation is pending
    if (this.pipeForm.pending) {
      this.pipeForm.setErrors({ cdSubmitButton: true });
      return;
    }

    const selectedSourceZones = this.pipeForm.getValue('source_zones');
    const selectedDestZones = this.pipeForm.getValue('destination_zones');
    if (this.editing) {
      destZones.removed = this.getZoneData(this.pipeSelectedRow.dest.zones, selectedDestZones);
      destZones.added = this.getZoneData(selectedDestZones, this.pipeSelectedRow.dest.zones);
      sourceZones.removed = this.getZoneData(
        this.pipeSelectedRow.source.zones,
        selectedSourceZones
      );
      sourceZones.added = this.getZoneData(selectedSourceZones, this.pipeSelectedRow.source.zones);
    }
    sourceZones.added = this.assignZoneValue(sourceZones.added, selectedSourceZones);
    destZones.added = this.assignZoneValue(destZones.added, selectedDestZones);

    sourceZones.removed = this.replaceWithAsterisk(sourceZones.removed);
    destZones.removed = this.replaceWithAsterisk(destZones.removed);

    this.rgwMultisiteService
      .createEditSyncPipe({
        ...this.pipeForm.getRawValue(),
        source_zones: sourceZones,
        destination_zones: destZones,
        user: this.editing ? this.pipeSelectedRow?.params?.user : '',
        mode: this.editing ? this.pipeSelectedRow?.params?.mode : ''
      })
      .subscribe({
        next: () => {
          const action = this.editing ? this.succeededLabels.EDITED : this.succeededLabels.CREATED;
          this.notificationService.show(
            NotificationType.success,
            $localize`${action} Sync Pipe '${this.pipeForm.getValue('pipe_id')}'`
          );
        },
        error: () => {
          // Reset the 'Submit' button.
          this.pipeForm.setErrors({ cdSubmitButton: true });
        },
        complete: () => {
          this.closeModal();
        }
      });
  }
}
