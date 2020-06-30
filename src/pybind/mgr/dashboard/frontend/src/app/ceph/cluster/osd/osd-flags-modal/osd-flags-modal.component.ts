import { Component, OnInit } from '@angular/core';
import { FormGroup } from '@angular/forms';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { I18n } from '@ngx-translate/i18n-polyfill';
import * as _ from 'lodash';

import { OsdService } from '../../../../shared/api/osd.service';
import { NotificationType } from '../../../../shared/enum/notification-type.enum';
import { Permissions } from '../../../../shared/models/permissions';
import { AuthStorageService } from '../../../../shared/services/auth-storage.service';
import { NotificationService } from '../../../../shared/services/notification.service';

@Component({
  selector: 'cd-osd-flags-modal',
  templateUrl: './osd-flags-modal.component.html',
  styleUrls: ['./osd-flags-modal.component.scss']
})
export class OsdFlagsModalComponent implements OnInit {
  permissions: Permissions;

  osdFlagsForm = new FormGroup({});

  allFlags = {
    noin: {
      code: 'noin',
      name: this.i18n('No In'),
      value: false,
      description: this.i18n(
        'OSDs that were previously marked out will not be marked back in when they start'
      )
    },
    noout: {
      code: 'noout',
      name: this.i18n('No Out'),
      value: false,
      description: this.i18n(
        'OSDs will not automatically be marked out after the configured interval'
      )
    },
    noup: {
      code: 'noup',
      name: this.i18n('No Up'),
      value: false,
      description: this.i18n('OSDs are not allowed to start')
    },
    nodown: {
      code: 'nodown',
      name: this.i18n('No Down'),
      value: false,
      description: this.i18n(
        'OSD failure reports are being ignored, such that the monitors will not mark OSDs down'
      )
    },
    pause: {
      code: 'pause',
      name: this.i18n('Pause'),
      value: false,
      description: this.i18n('Pauses reads and writes')
    },
    noscrub: {
      code: 'noscrub',
      name: this.i18n('No Scrub'),
      value: false,
      description: this.i18n('Scrubbing is disabled')
    },
    'nodeep-scrub': {
      code: 'nodeep-scrub',
      name: this.i18n('No Deep Scrub'),
      value: false,
      description: this.i18n('Deep Scrubbing is disabled')
    },
    nobackfill: {
      code: 'nobackfill',
      name: this.i18n('No Backfill'),
      value: false,
      description: this.i18n('Backfilling of PGs is suspended')
    },
    norebalance: {
      code: 'norebalance',
      name: this.i18n('No Rebalance'),
      value: false,
      description: this.i18n('OSD will choose not to backfill unless PG is also degraded')
    },
    norecover: {
      code: 'norecover',
      name: this.i18n('No Recover'),
      value: false,
      description: this.i18n('Recovery of PGs is suspended')
    },
    sortbitwise: {
      code: 'sortbitwise',
      name: this.i18n('Bitwise Sort'),
      value: false,
      description: this.i18n('Use bitwise sort'),
      disabled: true
    },
    purged_snapdirs: {
      code: 'purged_snapdirs',
      name: this.i18n('Purged Snapdirs'),
      value: false,
      description: this.i18n('OSDs have converted snapsets'),
      disabled: true
    },
    recovery_deletes: {
      code: 'recovery_deletes',
      name: this.i18n('Recovery Deletes'),
      value: false,
      description: this.i18n('Deletes performed during recovery instead of peering'),
      disabled: true
    },
    pglog_hardlimit: {
      code: 'pglog_hardlimit',
      name: this.i18n('PG Log Hard Limit'),
      value: false,
      description: this.i18n('Puts a hard limit on pg log length'),
      disabled: true
    }
  };
  flags: any[];
  unknownFlags: string[] = [];

  constructor(
    public activeModal: NgbActiveModal,
    private authStorageService: AuthStorageService,
    private osdService: OsdService,
    private notificationService: NotificationService,
    private i18n: I18n
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
        this.notificationService.show(NotificationType.success, this.i18n('Updated OSD Flags'));
        this.activeModal.close();
      },
      () => {
        this.activeModal.close();
      }
    );
  }
}
