import { Component, OnInit } from '@angular/core';
import { UntypedFormGroup } from '@angular/forms';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import _ from 'lodash';

import { OsdService } from '~/app/shared/api/osd.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { Flag } from '~/app/shared/models/flag';
import { Permissions } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { NotificationService } from '~/app/shared/services/notification.service';

@Component({
  selector: 'cd-osd-flags-indiv-modal',
  templateUrl: './osd-flags-indiv-modal.component.html',
  styleUrls: ['./osd-flags-indiv-modal.component.scss']
})
export class OsdFlagsIndivModalComponent implements OnInit {
  permissions: Permissions;
  selected: object[];
  initialSelection: Flag[] = [];
  osdFlagsForm = new UntypedFormGroup({});
  flags: Flag[] = [
    {
      code: 'noup',
      name: $localize`No Up`,
      description: $localize`OSDs are not allowed to start`,
      value: false,
      clusterWide: false,
      indeterminate: false
    },
    {
      code: 'nodown',
      name: $localize`No Down`,
      description: $localize`OSD failure reports are being ignored, such that the monitors will not mark OSDs down`,
      value: false,
      clusterWide: false,
      indeterminate: false
    },
    {
      code: 'noin',
      name: $localize`No In`,
      description: $localize`OSDs that were previously marked out will not be marked back in when they start`,
      value: false,
      clusterWide: false,
      indeterminate: false
    },
    {
      code: 'noout',
      name: $localize`No Out`,
      description: $localize`OSDs will not automatically be marked out after the configured interval`,
      value: false,
      clusterWide: false,
      indeterminate: false
    }
  ];
  clusterWideTooltip: string = $localize`The flag has been enabled for the entire cluster.`;

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
        this.notificationService.show(NotificationType.success, $localize`Updated OSD Flags`);
        this.activeModal.close();
      },
      () => {
        this.activeModal.close();
      }
    );
  }
}
