import { Component, OnInit } from '@angular/core';
import { FormGroup } from '@angular/forms';

import * as _ from 'lodash';
import { BsModalRef } from 'ngx-bootstrap';

import { OsdService } from '../../../../shared/api/osd.service';
import { NotificationType } from '../../../../shared/enum/notification-type.enum';
import { NotificationService } from '../../../../shared/services/notification.service';

@Component({
  selector: 'cd-osd-flags-modal',
  templateUrl: './osd-flags-modal.component.html',
  styleUrls: ['./osd-flags-modal.component.scss']
})
export class OsdFlagsModalComponent implements OnInit {
  osdFlagsForm = new FormGroup({});

  allFlags = {
    noin: {
      code: 'noin',
      name: 'No In',
      value: false,
      description: 'OSDs that were previously marked out will not be marked back in when they start'
    },
    noout: {
      code: 'noout',
      name: 'No Out',
      value: false,
      description: 'OSDs will not automatically be marked out after the configured interval'
    },
    noup: {
      code: 'noup',
      name: 'No Up',
      value: false,
      description: 'OSDs are not allowed to start'
    },
    nodown: {
      code: 'nodown',
      name: 'No Down',
      value: false,
      description:
        'OSD failure reports are being ignored, such that the monitors will not mark OSDs down'
    },
    pause: {
      code: 'pause',
      name: 'Pause',
      value: false,
      description: 'Pauses reads and writes'
    },
    noscrub: {
      code: 'noscrub',
      name: 'No Scrub',
      value: false,
      description: 'Scrubbing is disabled'
    },
    'nodeep-scrub': {
      code: 'nodeep-scrub',
      name: 'No Deep Scrub',
      value: false,
      description: 'Deep Scrubbing is disabled'
    },
    nobackfill: {
      code: 'nobackfill',
      name: 'No Backfill',
      value: false,
      description: 'Backfilling of PGs is suspended'
    },
    norecover: {
      code: 'norecover',
      name: 'No Recover',
      value: false,
      description: 'Recovery of PGs is suspended'
    },
    sortbitwise: {
      code: 'sortbitwise',
      name: 'Bitwise Sort',
      value: false,
      description: 'Use bitwise sort',
      disabled: true
    },
    purged_snapdirs: {
      code: 'purged_snapdirs',
      name: 'Purged Snapdirs',
      value: false,
      description: 'OSDs have converted snapsets',
      disabled: true
    },
    recovery_deletes: {
      code: 'recovery_deletes',
      name: 'Recovery Deletes',
      value: false,
      description: 'Deletes performed during recovery instead of peering',
      disabled: true
    }
  };
  flags: any[];
  unknownFlags: string[] = [];

  constructor(
    public bsModalRef: BsModalRef,
    private osdService: OsdService,
    private notificationService: NotificationService
  ) {}

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
        this.notificationService.show(
          NotificationType.success,
          'OSD Flags were updated successfully.',
          'OSD Flags'
        );
        this.bsModalRef.hide();
      },
      () => {
        this.bsModalRef.hide();
      }
    );
  }
}
