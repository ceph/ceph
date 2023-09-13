import { Component, OnInit } from '@angular/core';
import { UntypedFormGroup } from '@angular/forms';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import _ from 'lodash';

import { OsdService } from '~/app/shared/api/osd.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { Permissions } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { NotificationService } from '~/app/shared/services/notification.service';

@Component({
  selector: 'cd-osd-flags-modal',
  templateUrl: './osd-flags-modal.component.html',
  styleUrls: ['./osd-flags-modal.component.scss']
})
export class OsdFlagsModalComponent implements OnInit {
  permissions: Permissions;

  osdFlagsForm = new UntypedFormGroup({});

  allFlags = {
    noin: {
      code: 'noin',
      name: $localize`No In`,
      value: false,
      description: $localize`OSDs that were previously marked out will not be marked back in when they start`
    },
    noout: {
      code: 'noout',
      name: $localize`No Out`,
      value: false,
      description: $localize`OSDs will not automatically be marked out after the configured interval`
    },
    noup: {
      code: 'noup',
      name: $localize`No Up`,
      value: false,
      description: $localize`OSDs are not allowed to start`
    },
    nodown: {
      code: 'nodown',
      name: $localize`No Down`,
      value: false,
      description: $localize`OSD failure reports are being ignored, such that the monitors will not mark OSDs down`
    },
    pause: {
      code: 'pause',
      name: $localize`Pause`,
      value: false,
      description: $localize`Pauses reads and writes`
    },
    noscrub: {
      code: 'noscrub',
      name: $localize`No Scrub`,
      value: false,
      description: $localize`Scrubbing is disabled`
    },
    'nodeep-scrub': {
      code: 'nodeep-scrub',
      name: $localize`No Deep Scrub`,
      value: false,
      description: $localize`Deep Scrubbing is disabled`
    },
    nobackfill: {
      code: 'nobackfill',
      name: $localize`No Backfill`,
      value: false,
      description: $localize`Backfilling of PGs is suspended`
    },
    norebalance: {
      code: 'norebalance',
      name: $localize`No Rebalance`,
      value: false,
      description: $localize`OSD will choose not to backfill unless PG is also degraded`
    },
    norecover: {
      code: 'norecover',
      name: $localize`No Recover`,
      value: false,
      description: $localize`Recovery of PGs is suspended`
    },
    sortbitwise: {
      code: 'sortbitwise',
      name: $localize`Bitwise Sort`,
      value: false,
      description: $localize`Use bitwise sort`,
      disabled: true
    },
    purged_snapdirs: {
      code: 'purged_snapdirs',
      name: $localize`Purged Snapdirs`,
      value: false,
      description: $localize`OSDs have converted snapsets`,
      disabled: true
    },
    recovery_deletes: {
      code: 'recovery_deletes',
      name: $localize`Recovery Deletes`,
      value: false,
      description: $localize`Deletes performed during recovery instead of peering`,
      disabled: true
    },
    pglog_hardlimit: {
      code: 'pglog_hardlimit',
      name: $localize`PG Log Hard Limit`,
      value: false,
      description: $localize`Puts a hard limit on pg log length`,
      disabled: true
    }
  };
  flags: any[];
  unknownFlags: string[] = [];

  constructor(
    public activeModal: NgbActiveModal,
    public actionLabels: ActionLabelsI18n,
    private authStorageService: AuthStorageService,
    private osdService: OsdService,
    private notificationService: NotificationService
  ) {
    this.permissions = this.authStorageService.getPermissions();
  }

  ngOnInit() {
    this.osdService.getFlags().subscribe((res: string[]) => {
      res.forEach((value) => {
        if (this.allFlags[value]) {
          this.allFlags[value].value = true;
        } else {
          this.unknownFlags.push(value);
        }
      });
      this.flags = _.toArray(this.allFlags);
    });
  }

  submitAction() {
    const newFlags = this.flags
      .filter((flag) => flag.value)
      .map((flag) => flag.code)
      .concat(this.unknownFlags);

    this.osdService.updateFlags(newFlags).subscribe(
      () => {
        this.notificationService.show(NotificationType.success, $localize`Updated OSD Flags`);
        this.activeModal.close();
      },
      () => {
        this.activeModal.close();
      }
    );
  }
}
