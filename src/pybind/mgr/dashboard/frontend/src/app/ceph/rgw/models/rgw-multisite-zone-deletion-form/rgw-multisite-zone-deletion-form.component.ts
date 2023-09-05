import { AfterViewInit, Component, OnInit } from '@angular/core';
import { UntypedFormControl } from '@angular/forms';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { PoolService } from '~/app/shared/api/pool.service';
import { RgwZoneService } from '~/app/shared/api/rgw-zone.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { NotificationService } from '~/app/shared/services/notification.service';

@Component({
  selector: 'cd-rgw-multisite-zone-deletion-form',
  templateUrl: './rgw-multisite-zone-deletion-form.component.html',
  styleUrls: ['./rgw-multisite-zone-deletion-form.component.scss']
})
export class RgwMultisiteZoneDeletionFormComponent implements OnInit, AfterViewInit {
  zoneData$: any;
  poolList$: any;
  zone: any;
  zoneForm: CdFormGroup;
  displayText: boolean = false;
  includedPools: Set<string> = new Set<string>();

  constructor(
    public activeModal: NgbActiveModal,
    public actionLabels: ActionLabelsI18n,
    public notificationService: NotificationService,
    private rgwZoneService: RgwZoneService,
    private poolService: PoolService
  ) {
    this.createForm();
  }

  ngOnInit(): void {
    this.zoneData$ = this.rgwZoneService.get(this.zone);
    this.poolList$ = this.poolService.getList();
  }

  ngAfterViewInit(): void {
    this.updateIncludedPools();
  }

  createForm() {
    this.zoneForm = new CdFormGroup({
      deletePools: new UntypedFormControl(false)
    });
  }

  submit() {
    this.rgwZoneService
      .delete(this.zone.name, this.zoneForm.value.deletePools, this.includedPools, this.zone.parent)
      .subscribe(
        () => {
          this.notificationService.show(
            NotificationType.success,
            $localize`Zone: '${this.zone.name}' deleted successfully`
          );
          this.activeModal.close();
        },
        () => {
          this.zoneForm.setErrors({ cdSubmitButton: true });
        }
      );
  }

  showDangerText() {
    this.displayText = !this.displayText;
  }

  updateIncludedPools(): void {
    if (!this.zoneData$ || !this.poolList$) {
      return;
    }
    this.zoneData$.subscribe((data: any) => {
      this.poolList$.subscribe((poolList: any) => {
        for (const pool of poolList) {
          for (const zonePool of Object.values(data)) {
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
}
