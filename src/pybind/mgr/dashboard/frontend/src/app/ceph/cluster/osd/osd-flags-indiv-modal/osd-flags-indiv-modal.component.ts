import { Component, OnInit } from '@angular/core';
import { FormGroup } from '@angular/forms';

import { I18n } from '@ngx-translate/i18n-polyfill';
import * as _ from 'lodash';
import { BsModalRef } from 'ngx-bootstrap/modal';

import { OsdService } from '../../../../shared/api/osd.service';
import { NotificationType } from '../../../../shared/enum/notification-type.enum';
import { Flag } from '../../../../shared/models/flag';
import { Permissions } from '../../../../shared/models/permissions';
import { AuthStorageService } from '../../../../shared/services/auth-storage.service';
import { NotificationService } from '../../../../shared/services/notification.service';

@Component({
  selector: 'cd-osd-flags-indiv-modal',
  templateUrl: './osd-flags-indiv-modal.component.html',
  styleUrls: ['./osd-flags-indiv-modal.component.scss']
})
export class OsdFlagsIndivModalComponent implements OnInit {
  permissions: Permissions;
  selected: object[];
  initialSelection: Flag[] = [];
  osdFlagsForm = new FormGroup({});
  flags: Flag[] = [
    {
      code: 'noup',
      name: this.i18n('No Up'),
      description: this.i18n('OSDs are not allowed to start'),
      value: false,
      clusterWide: false,
      indeterminate: false
    },
    {
      code: 'nodown',
      name: this.i18n('No Down'),
      description: this.i18n(
        'OSD failure reports are being ignored, such that the monitors will not mark OSDs down'
      ),
      value: false,
      clusterWide: false,
      indeterminate: false
    },
    {
      code: 'noin',
      name: this.i18n('No In'),
      description: this.i18n(
        'OSDs that were previously marked out will not be marked back in when they start'
      ),
      value: false,
      clusterWide: false,
      indeterminate: false
    },
    {
      code: 'noout',
      name: this.i18n('No Out'),
      description: this.i18n(
        'OSDs will not automatically be marked out after the configured interval'
      ),
      value: false,
      clusterWide: false,
      indeterminate: false
    }
  ];
  clusterWideTooltip: string = this.i18n('The flag has been enabled for the entire cluster.');

  constructor(
    public activeModal: BsModalRef,
    private authStorageService: AuthStorageService,
    private osdService: OsdService,
    private notificationService: NotificationService,
    private i18n: I18n
  ) {
    this.permissions = this.authStorageService.getPermissions();
  }

  ngOnInit() {
    const osdCount = this.selected.length;
    this.osdService.getFlags().subscribe((clusterWideFlags: string[]) => {
      const activatedIndivFlags = this.getActivatedIndivFlags();
      this.flags.forEach((flag) => {
        const flagCount = activatedIndivFlags[flag.code];
        if (clusterWideFlags.includes(flag.code)) {
          flag.clusterWide = true;
        }

        if (flagCount === osdCount) {
          flag.value = true;
        } else if (flagCount > 0) {
          flag.indeterminate = true;
        }
      });
      this.initialSelection = _.cloneDeep(this.flags);
    });
  }

  getActivatedIndivFlags(): { [flag: string]: number } {
    const flagsCount = {};
    this.flags.forEach((flag) => {
      flagsCount[flag.code] = 0;
    });

    [].concat(...this.selected.map((osd) => osd['state'])).map((activatedFlag) => {
      if (Object.keys(flagsCount).includes(activatedFlag)) {
        flagsCount[activatedFlag] = flagsCount[activatedFlag] + 1;
      }
    });
    return flagsCount;
  }

  changeValue(flag: Flag) {
    flag.value = !flag.value;
    flag.indeterminate = false;
  }

  resetSelection() {
    this.flags = _.cloneDeep(this.initialSelection);
  }

  submitAction() {
    const activeFlags = {};
    this.flags.forEach((flag) => {
      if (flag.indeterminate) {
        activeFlags[flag.code] = null;
      } else {
        activeFlags[flag.code] = flag.value;
      }
    });
    const selectedIds = this.selected.map((selection) => selection['osd']);
    this.osdService.updateIndividualFlags(activeFlags, selectedIds).subscribe(
      () => {
        this.notificationService.show(NotificationType.success, this.i18n('Updated OSD Flags'));
        this.activeModal.hide();
      },
      () => {
        this.activeModal.hide();
      }
    );
  }
}
