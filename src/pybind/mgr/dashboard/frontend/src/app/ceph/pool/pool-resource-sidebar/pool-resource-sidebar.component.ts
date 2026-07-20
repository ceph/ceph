import { Component, OnDestroy, OnInit, ViewEncapsulation } from '@angular/core';
import { ActivatedRoute, ParamMap, Router } from '@angular/router';

import { Subscription } from 'rxjs';

import _ from 'lodash';
import { ConfigurationService } from '~/app/shared/api/configuration.service';
import { PoolService } from '~/app/shared/api/pool.service';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { SidebarItem } from '~/app/shared/components/sidebar-layout/sidebar-layout.component';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { DeletionImpact } from '~/app/shared/enum/delete-confirmation-modal-impact.enum';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { Permissions } from '~/app/shared/models/permissions';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { getPoolDataProtection, mapPoolApplications, Pool } from '../pool';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';

@Component({
  selector: 'cd-pool-resource-sidebar',
  templateUrl: './pool-resource-sidebar.component.html',
  styleUrls: ['./pool-resource-sidebar.component.scss'],
  encapsulation: ViewEncapsulation.None,
  standalone: false
})
export class PoolResourceSidebarComponent implements OnInit, OnDestroy {
  private sub = new Subscription();
  public readonly basePath = '/pool/view';
  poolName = '';
  poolDataProtection = '';
  poolApplications: string[] = [];
  poolActions: CdTableAction[] = [];
  poolSelection = new CdTableSelection();
  monAllowPoolDelete = false;
  sidebarItems: SidebarItem[] = [];
  permissions: Permissions;

  constructor(
    private route: ActivatedRoute,
    private router: Router,
    private authStorageService: AuthStorageService,
    private poolService: PoolService,
    private configurationService: ConfigurationService,
    private actionLabels: ActionLabelsI18n,
    private modalService: ModalCdsService,
    private taskWrapper: TaskWrapperService
  ) {
    this.permissions = this.authStorageService.getPermissions();
  }

  ngOnInit(): void {
    this.loadDeleteCapability();
    this.sub.add(
      this.route.paramMap.subscribe((pm: ParamMap) => {
        this.poolName = pm.get('name') ?? '';
        this.buildSidebarItems(this.permissions);
        this.loadPoolHeaderMetadata();
      })
    );

    this.buildPoolActions();
  }

  ngOnDestroy(): void {
    this.sub.unsubscribe();
  }

  private buildSidebarItems(permissions: any): void {
    const items: SidebarItem[] = [
      {
        label: $localize`Overview`,
        route: [this.basePath, this.poolName, 'overview'],
        routerLinkActiveOptions: { exact: true }
      },
      {
        label: $localize`Configuration`,
        route: [this.basePath, this.poolName, 'configuration'],
        routerLinkActiveOptions: { exact: true }
      }
    ];

    if (permissions.grafana?.read) {
      items.push({
        label: $localize`Performance`,
        route: [this.basePath, this.poolName, 'performance'],
        routerLinkActiveOptions: { exact: true }
      });
    }

    items.push({
      label: $localize`Advanced Properties`,
      route: [this.basePath, this.poolName, 'advanced-properties'],
      routerLinkActiveOptions: { exact: true }
    });

    this.sidebarItems = items;
  }

  private loadDeleteCapability(): void {
    if (this.permissions.configOpt?.read) {
      this.sub.add(
        this.configurationService.get('mon_allow_pool_delete').subscribe((data: any) => {
          if (_.has(data, 'value')) {
            const monSection = _.find(data.value, (v) => {
              return v.section === 'mon';
            }) || { value: false };
            this.monAllowPoolDelete = monSection.value === 'true';
          }
        })
      );
    } else if (this.permissions.pool?.read) {
      this.monAllowPoolDelete = true;
    }
  }

  private loadPoolHeaderMetadata(): void {
    if (!this.poolName) {
      this.poolDataProtection = '';
      this.poolApplications = [];
      this.poolSelection.selected = [];
      return;
    }

    this.sub.add(
      this.poolService.get(this.poolName).subscribe((pool: any) => {
        const poolData = pool as Pool;

        this.poolDataProtection = getPoolDataProtection(poolData);
        this.poolApplications = mapPoolApplications(poolData?.application_metadata || []);
        this.poolSelection.selected = poolData ? [poolData] : [];
        this.buildPoolActions();
      })
    );
  }

  private buildPoolActions(): void {
    this.poolActions = [
      {
        name: this.actionLabels.EDIT,
        permission: 'update',
        icon: Icons.edit,
        click: () => this.editPool(),
        disable: (selection: CdTableSelection) => !selection?.hasSingleSelection
      },
      {
        name: this.actionLabels.DELETE,
        permission: 'delete',
        icon: Icons.destroy,
        click: () => this.deletePool(),
        disable: (selection: CdTableSelection) => this.getDeleteDisable(selection)
      }
    ];
  }

  getVisiblePoolActions(): CdTableAction[] {
    return this.poolActions.filter((action) => {
      if (action.permission && !this.permissions.pool?.[action.permission]) {
        return false;
      }

      return !action.visible || action.visible(this.poolSelection);
    });
  }

  isPoolActionDisabled(action: CdTableAction): boolean {
    return !!action.disable?.(this.poolSelection);
  }

  runPoolAction(action: CdTableAction): void {
    if (this.isPoolActionDisabled(action)) {
      return;
    }

    action.click?.();
  }

  private editPool(): void {
    if (!this.poolName) {
      return;
    }

    this.router.navigate(['/pool', URLVerbs.EDIT, this.poolName]);
  }

  private deletePool(): void {
    const selectedPool = this.poolSelection.first();
    const name = selectedPool?.pool_name || this.poolName;
    if (!name) {
      return;
    }

    this.modalService.show(DeleteConfirmationModalComponent, {
      impact: DeletionImpact.high,
      itemDescription: 'Pool',
      itemNames: [name],
      submitActionObservable: () =>
        this.taskWrapper.wrapTaskAroundCall({
          task: new FinishedTask(`pool/${URLVerbs.DELETE}`, { pool_name: name }),
          call: this.poolService.delete(name)
        })
    });
  }

  private getDeleteDisable(selection: CdTableSelection): boolean | string {
    if (!selection?.hasSingleSelection) {
      return true;
    }

    if (!this.monAllowPoolDelete) {
      return $localize`Pool deletion is disabled by the mon_allow_pool_delete configuration setting.`;
    }

    return false;
  }
}
