import { Component, inject, Input, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { Observable, BehaviorSubject, forkJoin, of } from 'rxjs';
import { switchMap, catchError, map } from 'rxjs/operators';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { Permission } from '~/app/shared/models/permissions';
import { SMBCluster, SMBShare } from '../smb.model';
import { SmbService } from '~/app/shared/api/smb.service';
import { RgwBucketService } from '~/app/shared/api/rgw-bucket.service';
import { Bucket } from '~/app/ceph/rgw/models/rgw-bucket';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { SMB_CONTEXT, StorageBackend } from '~/app/shared/constants/smb.constant';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';

interface SMBShareRow extends SMBShare {
  bucketRouterLink?: string[];
  hasPersistentHandles?: boolean;
}

@Component({
  selector: 'cd-smb-share-list',
  templateUrl: './smb-share-list.component.html',
  styleUrls: ['./smb-share-list.component.scss'],
  standalone: false
})
export class SmbShareListComponent implements OnInit {
  private readonly ctx = inject(SMB_CONTEXT);
  private readonly clusterPath = `${this.ctx.basePath}/cluster`;
  private readonly sharePath = `${this.ctx.basePath}/share`;

  @Input()
  clusterId: string;
  @ViewChild('table', { static: true })
  table: TableComponent;
  @ViewChild('iopsLimitTpl', { static: true })
  iopsLimitTpl: TemplateRef<any>;
  @ViewChild('bwLimitTpl', { static: true })
  bwLimitTpl: TemplateRef<any>;
  @ViewChild('delayMaxTpl', { static: true })
  delayMaxTpl: TemplateRef<any>;
  columns: CdTableColumn[];
  permission: Permission;
  selection = new CdTableSelection();
  tableActions: CdTableAction[];
  context: CdTableFetchDataContext;

  smbShares$: Observable<SMBShareRow[] | null>;
  subject$ = new BehaviorSubject<SMBShareRow[]>([]);
  modalRef: NgbModalRef;
  isRgw = false;
  shareListDescription = '';
  private persistentHandlesDefault = false;

  constructor(
    private authStorageService: AuthStorageService,
    public actionLabels: ActionLabelsI18n,
    private smbService: SmbService,
    private rgwBucketService: RgwBucketService,
    private taskWrapper: TaskWrapperService,
    private modalService: ModalCdsService
  ) {
    this.permission = this.authStorageService.getPermissions().smb;
  }

  ngOnInit() {
    this.isRgw = this.ctx.backend === StorageBackend.RGW;
    this.shareListDescription = this.isRgw
      ? $localize`Logical unit hosted by the cluster that maps to an RGW bucket`
      : $localize`Logical unit hosted by the cluster that maps to the given CephFS volume and path`;
    this.columns = this.isRgw
      ? [
          {
            name: $localize`ID`,
            prop: 'share_id',
            flexGrow: 2
          },
          {
            name: $localize`Name`,
            prop: 'name',
            flexGrow: 2
          },
          {
            name: $localize`Bucket`,
            prop: 'rgw.bucket',
            flexGrow: 2,
            cellTransformation: CellTemplate.redirect,
            customTemplateConfig: {
              customRowProperty: 'bucketRouterLink'
            }
          },
          {
            name: $localize`Read Only`,
            prop: 'readonly',
            flexGrow: 1,
            cellTransformation: CellTemplate.checkIcon
          },
          {
            name: $localize`Browsable`,
            prop: 'browseable',
            flexGrow: 1,
            cellTransformation: CellTemplate.checkIcon
          }
        ]
      : [
          {
            name: $localize`ID`,
            prop: 'share_id',
            flexGrow: 2
          },
          {
            name: $localize`Name`,
            prop: 'name',
            flexGrow: 2
          },
          {
            name: $localize`File System`,
            prop: 'cephfs.volume',
            flexGrow: 2
          },
          {
            name: $localize`Path`,
            prop: 'cephfs.path',
            cellTransformation: CellTemplate.path,
            flexGrow: 2
          },
          {
            name: $localize`Subvolume group`,
            prop: 'cephfs.subvolumegroup',
            flexGrow: 2
          },
          {
            name: $localize`Subvolume`,
            prop: 'cephfs.subvolume',
            flexGrow: 2
          },
          {
            name: $localize`Provider`,
            prop: 'cephfs.provider',
            flexGrow: 2
          },
          {
            name: $localize`Persistent handles`,
            prop: 'hasPersistentHandles',
            flexGrow: 2,
            cellTransformation: CellTemplate.checkIcon
          },
          {
            name: $localize`IOPS Limit`,
            prop: 'cephfs.qos',
            cellTemplate: this.iopsLimitTpl,
            flexGrow: 2
          },
          {
            name: $localize`Bandwidth Limit`,
            prop: 'cephfs.qos',
            cellTemplate: this.bwLimitTpl,
            flexGrow: 2
          },
          {
            name: $localize`Delay Max`,
            prop: 'cephfs.qos',
            cellTemplate: this.delayMaxTpl,
            flexGrow: 2
          }
        ];
    const createAction: CdTableAction = {
      name: `${this.actionLabels.CREATE}`,
      permission: 'create',
      icon: Icons.add,
      routerLink: () => [`/${this.clusterPath}`, this.clusterId, URLVerbs.CREATE],
      canBePrimary: (selection: CdTableSelection) => !selection.hasSingleSelection
    };
    const editAction: CdTableAction = {
      name: this.actionLabels.EDIT,
      permission: 'update',
      icon: Icons.edit,
      routerLink: () => [
        `/${this.clusterPath}`,
        this.clusterId,
        URLVerbs.EDIT,
        this.selection.first().share_id
      ]
    };
    const deleteAction: CdTableAction = {
      permission: 'delete',
      icon: Icons.destroy,
      click: () => this.deleteShareModal(),
      name: this.actionLabels.DELETE
    };

    this.tableActions = [createAction, editAction, deleteAction];

    this.smbShares$ = this.subject$.pipe(
      switchMap(() =>
        forkJoin({
          shares: this.smbService.listShares(this.clusterId),
          persistentHandlesDefault: this.loadPersistentHandlesDefault()
        }).pipe(
          map(({ shares, persistentHandlesDefault }) => {
            this.persistentHandlesDefault = persistentHandlesDefault;
            return this.filterByBackend(shares);
          }),
          switchMap((shares) => this.withBucketLinks(shares)),
          catchError(() => {
            this.context.error();
            return of(null);
          })
        )
      )
    );
  }

  private loadPersistentHandlesDefault(): Observable<boolean> {
    if (this.isRgw) {
      return of(false);
    }
    return this.smbService.getCluster(this.clusterId).pipe(
      map((cluster: SMBCluster) => !!cluster.share_defaults?.availability?.persistent_handles),
      catchError(() => of(false))
    );
  }

  private filterByBackend(shares: SMBShare[]): SMBShare[] {
    if (!shares?.length) {
      return shares || [];
    }
    return shares.filter((share) => (this.isRgw ? !!share.rgw : !!share.cephfs));
  }

  private withBucketLinks(shares: SMBShare[]): Observable<SMBShareRow[]> {
    if (!shares?.length) {
      return of([]);
    }
    if (!this.isRgw) {
      return of(this.mapShareRows(shares));
    }

    const needsOwnerLookup = shares.some((share) => share.rgw?.bucket && !share.rgw?.user_id);
    if (!needsOwnerLookup) {
      return of(this.mapShareRows(shares));
    }
    return this.rgwBucketService.list(true).pipe(
      map((buckets: Bucket[]) => {
        const ownerByBid = new Map(
          (buckets || []).map((bucket) => [bucket.bid || bucket.bucket, bucket.owner])
        );
        return this.mapShareRows(shares, ownerByBid);
      }),
      catchError(() => of(this.mapShareRows(shares)))
    );
  }

  private mapShareRows(shares: SMBShare[], ownerByBid?: Map<string, string>): SMBShareRow[] {
    return shares.map((share) => {
      const bucketName = share.rgw?.bucket;
      const owner = share.rgw?.user_id || (bucketName && ownerByBid?.get(bucketName));
      // Shares without an explicit override inherit the cluster default.
      const hasPersistentHandles =
        !!share.cephfs && (share.availability?.persistent_handles ?? this.persistentHandlesDefault);
      return {
        ...share,
        hasPersistentHandles,
        bucketRouterLink: bucketName && owner ? ['/rgw/bucket', bucketName, owner] : undefined
      };
    });
  }

  loadSMBShares() {
    this.subject$.next([]);
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  deleteShareModal() {
    const cluster_id = this.selection.first().cluster_id;
    const share_id = this.selection.first().share_id;
    const name = this.selection.first().name;

    this.modalRef = this.modalService.show(DeleteConfirmationModalComponent, {
      itemDescription: $localize`SMB Share`,
      itemNames: [`Share: ${share_id} (${name}) from cluster: ${cluster_id}`],
      submitActionObservable: () =>
        this.taskWrapper.wrapTaskAroundCall({
          task: new FinishedTask(`${this.sharePath}/${URLVerbs.DELETE}`, {
            share_id: share_id
          }),
          call: this.smbService.deleteShare(cluster_id, share_id)
        })
    });
  }
}
