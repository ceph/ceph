import {
  Component,
  DestroyRef,
  inject,
  Input,
  OnChanges,
  OnInit,
  SimpleChanges,
  ViewChild
} from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Icons } from '~/app/shared/enum/icons.enum';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { RgwRoleService } from '~/app/shared/api/rgw-role.service';
import { RgwDaemonService } from '~/app/shared/api/rgw-daemon.service';
import { RgwZonegroupService } from '~/app/shared/api/rgw-zonegroup.service';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { RgwAccountRoleFormComponent } from '../rgw-account-role-form/rgw-account-role-form.component';
import { Observable, Subscriber, of } from 'rxjs';
import { catchError, distinctUntilChanged, filter, map, switchMap } from 'rxjs/operators';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';

import { CdDatePipe } from '~/app/shared/pipes/cd-date.pipe';
import { DurationPipe } from '~/app/shared/pipes/duration.pipe';
import { RgwRole } from '../models/rgw-role';
import { RgwDaemon } from '../models/rgw-daemon';
import { RgwZonegroup } from '../models/rgw-multisite';

@Component({
  selector: 'cd-rgw-account-roles-list',
  templateUrl: './rgw-account-roles-list.component.html',
  styleUrls: ['./rgw-account-roles-list.component.scss'],
  standalone: false
})
export class RgwAccountRolesListComponent implements OnInit, OnChanges {
  @Input()
  accountId: string;

  @Input()
  accountName: string;

  @ViewChild('table')
  table: TableComponent;

  columns: CdTableColumn[] = [];
  data$: Observable<RgwRole[]>;
  tableActions: CdTableAction[] = [];
  selection: CdTableSelection = new CdTableSelection();
  permission: Permission;
  // Fail open until we know the selected daemon is on a secondary zone.
  isMasterZone = true;

  private readonly destroyRef = inject(DestroyRef);
  private readonly secondaryZoneDisableMsg = $localize`Role changes must be made on the master zone`;

  constructor(
    public actionLabels: ActionLabelsI18n,
    private rgwRoleService: RgwRoleService,
    private rgwDaemonService: RgwDaemonService,
    private rgwZonegroupService: RgwZonegroupService,
    private modalService: ModalCdsService,
    private authStorageService: AuthStorageService,
    private cdDatePipe: CdDatePipe,
    private durationPipe: DurationPipe,
    private notificationService: NotificationService
  ) {
    this.permission = this.authStorageService.getPermissions().rgw;
  }

  ngOnInit(): void {
    this.loadRoles();
    this.resolveMasterZone();
    this.columns = [
      {
        name: $localize`Role name`,
        prop: 'RoleName',
        flexGrow: 2
      },
      {
        name: $localize`Path`,
        prop: 'Path',
        flexGrow: 2
      },
      {
        name: $localize`Arn`,
        prop: 'Arn',
        flexGrow: 3
      },
      {
        name: $localize`Created at`,
        prop: 'CreateDate',
        flexGrow: 2,
        pipe: this.cdDatePipe
      },
      {
        name: $localize`Max session duration`,
        prop: 'MaxSessionDuration',
        flexGrow: 2,
        pipe: this.durationPipe
      }
    ];

    this.tableActions = [
      {
        permission: 'create',
        icon: Icons.add,
        click: () => this.openRoleForm(false),
        name: this.actionLabels.CREATE,
        canBePrimary: (selection: CdTableSelection) => !selection.hasSelection,
        disable: () => this.getRoleActionDisable(false)
      },
      {
        permission: 'update',
        icon: Icons.edit,
        click: () => this.openRoleForm(true),
        name: this.actionLabels.EDIT,
        disable: () => this.getRoleActionDisable(true)
      },
      {
        permission: 'delete',
        icon: Icons.destroy,
        click: () => this.deleteRole(),
        name: this.actionLabels.DELETE,
        disable: () => this.getRoleActionDisable(true)
      }
    ];
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes.accountId) {
      this.loadRoles();
    }
  }

  loadRoles(): void {
    if (!this.accountId) {
      this.data$ = of([]);
      return;
    }
    this.data$ = this.rgwRoleService.list(this.accountId);
  }

  updateSelection(selection: CdTableSelection): void {
    this.selection = selection;
  }

  getRoleActionDisable(requireSelection: boolean): boolean | string {
    if (!this.isMasterZone) {
      return this.secondaryZoneDisableMsg;
    }
    if (requireSelection && !this.selection.hasSelection) {
      return true;
    }
    return false;
  }

  private resolveMasterZone(): void {
    this.rgwDaemonService.selectedDaemon$
      .pipe(
        filter((daemon): daemon is RgwDaemon => !!(daemon?.zone_name && daemon?.zonegroup_name)),
        distinctUntilChanged(
          (prev, curr) =>
            prev.zone_name === curr.zone_name && prev.zonegroup_name === curr.zonegroup_name
        ),
        switchMap((daemon) => {
          const zonegroup = new RgwZonegroup();
          zonegroup.name = daemon.zonegroup_name;
          return this.rgwZonegroupService.get(zonegroup).pipe(
            map((zg) => this.isDaemonOnMasterZone(daemon, zg as RgwZonegroup)),
            // Fail open on API errors / single-site setups.
            catchError(() => of(true))
          );
        }),
        takeUntilDestroyed(this.destroyRef)
      )
      .subscribe((isMasterZone) => {
        this.isMasterZone = isMasterZone;
      });
  }

  /** True when the selected daemon is on the zonegroup master zone (or topology is unknown). */
  private isDaemonOnMasterZone(daemon: RgwDaemon, zonegroup: RgwZonegroup): boolean {
    if (!zonegroup?.master_zone || !zonegroup?.zones?.length) {
      return true;
    }
    const masterZone = zonegroup.zones.find((zone) => zone.id === zonegroup.master_zone);
    return !masterZone || masterZone.name === daemon.zone_name;
  }

  openRoleForm(isEdit: boolean): void {
    if (!this.isMasterZone) {
      return;
    }
    const role = isEdit ? this.selection.first() : null;
    const modalRef = this.modalService.show(RgwAccountRoleFormComponent, {
      accountId: this.accountId,
      accountName: this.accountName,
      roleName: role ? role.RoleName : '',
      isEdit: isEdit,
      role: role
    });
    modalRef?.close?.subscribe(() => this.loadRoles());
  }

  deleteRole(): void {
    if (!this.isMasterZone || !this.selection.hasSelection) {
      return;
    }
    const roleName = this.selection.first().RoleName;
    this.modalService.show(DeleteConfirmationModalComponent, {
      itemDescription: $localize`Role`,
      itemNames: [roleName],
      submitActionObservable: () => {
        return new Observable((observer: Subscriber<any>) => {
          this.rgwRoleService.delete(roleName, this.accountId).subscribe({
            next: () => {
              this.notificationService.show(
                NotificationType.success,
                $localize`Role deleted successfully`
              );
              observer.next();
              observer.complete();
              this.loadRoles();
            },
            error: (err) => {
              observer.error(err);
            }
          });
        });
      }
    });
  }
}
