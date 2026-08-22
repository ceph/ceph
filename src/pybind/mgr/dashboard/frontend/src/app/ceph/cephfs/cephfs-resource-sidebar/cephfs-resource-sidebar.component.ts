import { Component, OnDestroy, OnInit, ViewEncapsulation } from '@angular/core';
import { ActivatedRoute, ParamMap, Router } from '@angular/router';
import { Subscription } from 'rxjs';

import {
  ResourceHeaderAction,
  ResourceHeaderStatus
} from '~/app/shared/components/page-header-resource/page-header-resource.component';
import { SidebarItem } from '~/app/shared/components/sidebar-layout/sidebar-layout.component';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CephfsDetail } from '~/app/shared/models/cephfs.model';
import { Permissions } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { CephfsActionService } from '~/app/shared/services/cephfs-action.service';
import { CephfsResourceStateService } from '~/app/shared/services/cephfs-resource-state.service';

@Component({
  selector: 'cd-cephfs-resource-sidebar',
  templateUrl: './cephfs-resource-sidebar.component.html',
  styleUrls: ['./cephfs-resource-sidebar.component.scss'],
  providers: [CephfsResourceStateService],
  encapsulation: ViewEncapsulation.None,
  standalone: false
})
export class CephfsResourceSidebarComponent implements OnInit, OnDestroy {
  private sub = new Subscription();
  readonly basePath = '/cephfs/fs';
  fsId = '';
  fsName = '';
  filesystem: CephfsDetail | null = null;
  sidebarItems: SidebarItem[] = [];
  headerStatus?: ResourceHeaderStatus;
  headerActions: ResourceHeaderAction[] = [];
  permissions: Permissions;
  monAllowPoolDelete = false;

  constructor(
    private route: ActivatedRoute,
    private router: Router,
    private authStorageService: AuthStorageService,
    public actionLabels: ActionLabelsI18n,
    private cephfsActionService: CephfsActionService,
    private cephfsResourceStateService: CephfsResourceStateService
  ) {
    this.permissions = this.authStorageService.getPermissions();
  }

  ngOnInit(): void {
    if (this.permissions?.configOpt?.read) {
      this.sub.add(
        this.cephfsActionService
          .getMonAllowPoolDelete()
          .subscribe((monAllowPoolDelete: boolean) => {
            this.monAllowPoolDelete = monAllowPoolDelete;
            this.updateHeaderActions();
          })
      );
    }

    this.sub.add(
      this.route.paramMap.subscribe((pm: ParamMap) => {
        this.fsId = pm.get('id') ?? '';
        this.buildSidebarItems();
        this.cephfsResourceStateService.load(this.fsId);
      })
    );

    this.sub.add(
      this.cephfsResourceStateService.filesystem$.subscribe((filesystem) => {
        this.filesystem = filesystem;
        this.fsName = filesystem?.mdsmap?.fs_name || filesystem?.cephfs?.name || this.fsId;
        this.headerStatus = this.getHeaderStatus(filesystem);
        this.updateHeaderActions();
      })
    );
  }

  ngOnDestroy(): void {
    this.sub.unsubscribe();
  }

  private buildSidebarItems(): void {
    this.sidebarItems = [
      {
        label: $localize`Overview`,
        route: [this.basePath, this.fsId, 'overview'],
        routerLinkActiveOptions: { exact: true }
      },
      {
        label: $localize`Directories`,
        route: [this.basePath, this.fsId, 'directories'],
        routerLinkActiveOptions: { exact: true }
      },
      {
        label: $localize`Subvolumes`,
        route: [this.basePath, this.fsId, 'subvolumes'],
        routerLinkActiveOptions: { exact: true }
      },
      {
        label: $localize`Subvolume groups`,
        route: [this.basePath, this.fsId, 'subvolume-groups'],
        routerLinkActiveOptions: { exact: true }
      },
      {
        label: $localize`Snapshots`,
        route: [this.basePath, this.fsId, 'snapshots'],
        routerLinkActiveOptions: { exact: true }
      },
      {
        label: $localize`Snapshot schedules`,
        route: [this.basePath, this.fsId, 'snapshot-schedules'],
        routerLinkActiveOptions: { exact: true }
      },
      {
        label: $localize`Clients`,
        route: [this.basePath, this.fsId, 'clients'],
        routerLinkActiveOptions: { exact: true }
      },
      {
        label: $localize`Performance`,
        route: [this.basePath, this.fsId, 'performance'],
        routerLinkActiveOptions: { exact: true }
      }
    ];
  }

  private getHeaderStatus(filesystem: CephfsDetail | null): ResourceHeaderStatus | undefined {
    if (!filesystem) {
      return undefined;
    }

    const enabled = !!filesystem?.mdsmap?.enabled;
    return {
      type: enabled ? 'success' : 'danger',
      text: enabled ? $localize`Enabled` : $localize`Disabled`
    };
  }

  private updateHeaderActions(): void {
    const fs = this.filesystem;
    const hasFilesystem = !!fs;
    const fsId = fs?.id ?? Number(this.fsId);

    this.headerActions = [
      {
        label: this.actionLabels.EDIT,
        disabled: !hasFilesystem || !this.permissions?.cephfs?.update,
        onClick: () => this.router.navigate([this.basePath, 'edit', String(fsId)])
      },
      {
        label: this.actionLabels.AUTHORIZE,
        disabled: !hasFilesystem || !this.permissions?.cephfs?.update,
        onClick: () => this.cephfsActionService.authorize(this.filesystem)
      },
      {
        label: this.actionLabels.ATTACH,
        disabled: !hasFilesystem || !this.permissions?.cephfs?.read,
        onClick: () => this.cephfsActionService.showAttachInfo(this.filesystem)
      },
      {
        label: this.actionLabels.REMOVE,
        disabled: !this.permissions?.cephfs?.delete || this.getDisableDesc() !== false,
        onClick: () => this.cephfsActionService.removeVolume(fs?.mdsmap?.fs_name || '')
      }
    ];
  }

  private getDisableDesc(): boolean | string {
    return this.cephfsActionService.getDeleteDisableDesc(
      !!this.filesystem,
      this.monAllowPoolDelete
    );
  }
}
