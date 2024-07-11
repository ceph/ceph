import { Component, OnInit } from '@angular/core';
import { UntypedFormControl, Validators } from '@angular/forms';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { RgwZonegroup } from '../models/rgw-multisite';
import { SelectOption } from '~/app/shared/components/select/select-option.model';
import { catchError, switchMap } from 'rxjs/operators';
import { of } from 'rxjs';
import { RgwDaemon } from '../models/rgw-daemon';
import { RgwDaemonService } from '~/app/shared/api/rgw-daemon.service';
import { RgwZonegroupService } from '~/app/shared/api/rgw-zonegroup.service';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import _ from 'lodash';
import { Icons } from '~/app/shared/enum/icons.enum';
import { RgwMultisiteService } from '~/app/shared/api/rgw-multisite.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { NotificationService } from '~/app/shared/services/notification.service';
import { ZoneData } from '../models/rgw-multisite-zone-selector';

@Component({
  selector: 'cd-rgw-multisite-sync-pipe-modal',
  templateUrl: './rgw-multisite-sync-pipe-modal.component.html',
  styleUrls: ['./rgw-multisite-sync-pipe-modal.component.scss']
})
export class RgwMultisiteSyncPipeModalComponent implements OnInit {
  groupExpandedRow: any;
  pipeSelectedRow: any;
  pipeForm: CdFormGroup;
  action: string;
  editing: boolean;
  sourceZones = new ZoneData(false, 'Filter Zones');
  destZones = new ZoneData(true, 'Filter or Add Zones');
  icons = Icons;

  constructor(
    public activeModal: NgbActiveModal,
    private rgwDaemonService: RgwDaemonService,
    private rgwZonegroupService: RgwZonegroupService,
    private rgwMultisiteService: RgwMultisiteService,
    private notificationService: NotificationService
  ) {}

  ngOnInit(): void {
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
    this.pipeForm.get('bucket_name').disable();
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
      .subscribe((zonegroupData: any) => {
        if (zonegroupData && zonegroupData?.zones?.length > 0) {
          let zones: any[] = [];
          zonegroupData.zones.forEach((zone: any) => {
            zones.push(new SelectOption(false, zone.name, ''));
          });
          this.sourceZones.data.available = [...zones];
          if (this.editing) {
            this.sourceZones.data.selected = this.pipeSelectedRow.source.zones;
            this.destZones.data.selected = this.pipeSelectedRow.dest.zones;
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

  onZoneSelection(zoneType: string) {
    if (zoneType === 'source_zones') {
      this.pipeForm.patchValue({
        source_zones: this.sourceZones.data.selected
      });
    } else {
      this.pipeForm.patchValue({
        destination_zones: this.destZones.data.selected
      });
    }
  }

  submit() {
    if (this.pipeForm.invalid) {
      return;
    }
    // Ensure that no validation is pending
    if (this.pipeForm.pending) {
      this.pipeForm.setErrors({ cdSubmitButton: true });
      return;
    }
    this.rgwMultisiteService.createEditSyncPipe(this.pipeForm.getRawValue()).subscribe(
      () => {
        const action = this.editing ? 'Modified' : 'Created';
        this.notificationService.show(
          NotificationType.success,
          $localize`${action} Sync Pipe '${this.pipeForm.getValue('pipe_id')}'`
        );
        this.activeModal.close('success');
      },
      () => {
        // Reset the 'Submit' button.
        this.pipeForm.setErrors({ cdSubmitButton: true });
        this.activeModal.dismiss();
      }
    );
  }
}
