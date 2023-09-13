import { AfterViewInit, Component, OnInit } from '@angular/core';
import { UntypedFormControl } from '@angular/forms';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { PoolService } from '~/app/shared/api/pool.service';
import { RgwZoneService } from '~/app/shared/api/rgw-zone.service';
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
export class RgwMultisiteZonegroupDeletionFormComponent implements OnInit, AfterViewInit {
  zonegroupData$: any;
  poolList$: any;
  zonesPools: Array<any> = [];
  zonegroup: any;
  zonesList: Array<any> = [];
  zonegroupForm: CdFormGroup;
  displayText: boolean = false;
  includedPools: Set<string> = new Set<string>();

  constructor(
    public activeModal: NgbActiveModal,
    public actionLabels: ActionLabelsI18n,
    public notificationService: NotificationService,
    private rgwZonegroupService: RgwZonegroupService,
    private poolService: PoolService,
    private rgwZoneService: RgwZoneService
  ) {
    this.createForm();
  }

  ngOnInit(): void {
    this.zonegroupData$ = this.rgwZonegroupService.get(this.zonegroup);
    this.poolList$ = this.poolService.getList();
  }

  ngAfterViewInit(): void {
    this.updateIncludedPools();
  }

  createForm() {
    this.zonegroupForm = new CdFormGroup({
      deletePools: new UntypedFormControl(false)
    });
  }

  submit() {
    this.rgwZonegroupService
      .delete(this.zonegroup.name, this.zonegroupForm.value.deletePools, this.includedPools)
      .subscribe(() => {
        this.notificationService.show(
          NotificationType.success,
          $localize`Zone: '${this.zonegroup.name}' deleted successfully`
        );
        this.activeModal.close();
      });
  }

  showDangerText() {
    if (this.includedPools.size > 0) {
      this.displayText = !this.displayText;
    }
  }

  updateIncludedPools(): void {
    if (!this.zonegroupData$ || !this.poolList$) {
      return;
    }

    this.zonegroupData$.subscribe((zgData: any) => {
      for (const zone of zgData.zones) {
        this.zonesList.push(zone.name);
        this.rgwZoneService.get(zone).subscribe((zonesPools: any) => {
          this.poolList$.subscribe((poolList: any) => {
            for (const zonePool of Object.values(zonesPools)) {
              for (const pool of poolList) {
                if (typeof zonePool === 'string' && zonePool.includes(pool.pool_name)) {
                  this.includedPools.add(pool.pool_name);
                } else if (Array.isArray(zonePool) && zonePool[0].val) {
                  for (const item of zonePool) {
                    const val = item.val;
                    if (val.storage_classes.STANDARD.data_pool === pool.pool_name) {
                      this.includedPools.add(val.storage_classes.STANDARD.data_pool);
                    }
                    if (val.data_extra_pool === pool.pool_name) {
                      this.includedPools.add(val.data_extra_pool);
                    }
                    if (val.index_pool === pool.pool_name) {
                      this.includedPools.add(val.index_pool);
                    }
                  }
                }
              }
            }
          });
        });
      }
    });
  }
}
