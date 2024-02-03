import { Component, Input, OnChanges, OnInit, SimpleChanges, ViewChild } from '@angular/core';
import { BehaviorSubject, Observable, of } from 'rxjs';
import { catchError, shareReplay, switchMap } from 'rxjs/operators';

import { CephfsSubvolumeGroupService } from '~/app/shared/api/cephfs-subvolume-group.service';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { CephfsSubvolumegroupFormComponent } from '../cephfs-subvolumegroup-form/cephfs-subvolumegroup-form.component';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalService } from '~/app/shared/services/modal.service';
import { Permissions } from '~/app/shared/models/permissions';
import { CriticalConfirmationModalComponent } from '~/app/shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { CephfsSubvolumeGroup } from '~/app/shared/models/cephfs-subvolume-group.model';

@Component({
  selector: 'cd-cephfs-subvolume-group',
  templateUrl: './cephfs-subvolume-group.component.html',
  styleUrls: ['./cephfs-subvolume-group.component.scss']
})
export class CephfsSubvolumeGroupComponent implements OnInit, OnChanges {
  @ViewChild('quotaUsageTpl', { static: true })
  quotaUsageTpl: any;

  @ViewChild('typeTpl', { static: true })
  typeTpl: any;

  @ViewChild('modeToHumanReadableTpl', { static: true })
  modeToHumanReadableTpl: any;

  @ViewChild('nameTpl', { static: true })
  nameTpl: any;

  @ViewChild('quotaSizeTpl', { static: true })
  quotaSizeTpl: any;

  @Input()
  fsName: any;
  @Input() pools: any[];

  columns: CdTableColumn[];
  tableActions: CdTableAction[];
  context: CdTableFetchDataContext;
  selection = new CdTableSelection();
  icons = Icons;
  permissions: Permissions;

  subvolumeGroup$: Observable<CephfsSubvolumeGroup[]>;
  subject = new BehaviorSubject<CephfsSubvolumeGroup[]>([]);

  constructor(
    private cephfsSubvolumeGroup: CephfsSubvolumeGroupService,
    private actionLabels: ActionLabelsI18n,
    private modalService: ModalService,
    private authStorageService: AuthStorageService,
    private taskWrapper: TaskWrapperService
  ) {
    this.permissions = this.authStorageService.getPermissions();
  }

  ngOnInit(): void {
    this.columns = [
      {
        name: $localize`Name`,
        prop: 'name',
        flexGrow: 0.6,
        cellTransformation: CellTemplate.bold
      },
      {
        name: $localize`Data Pool`,
        prop: 'info.data_pool',
        flexGrow: 0.7,
        cellTransformation: CellTemplate.badge,
        customTemplateConfig: {
          class: 'badge-background-primary'
        }
      },
      {
        name: $localize`Usage`,
        prop: 'info.bytes_pcent',
        flexGrow: 0.7,
        cellTemplate: this.quotaUsageTpl,
        cellClass: 'text-right'
      },
      {
        name: $localize`Mode`,
        prop: 'info.mode',
        flexGrow: 0.5,
        cellTemplate: this.modeToHumanReadableTpl
      },
      {
        name: $localize`Created`,
        prop: 'info.created_at',
        flexGrow: 0.5,
        cellTransformation: CellTemplate.timeAgo
      }
    ];

    this.tableActions = [
      {
        name: this.actionLabels.CREATE,
        permission: 'create',
        icon: Icons.add,
        click: () => this.openModal(),
        canBePrimary: (selection: CdTableSelection) => !selection.hasSelection
      },
      {
        name: this.actionLabels.EDIT,
        permission: 'update',
        icon: Icons.edit,
        click: () => this.openModal(true)
      },
      {
        name: this.actionLabels.REMOVE,
        permission: 'delete',
        icon: Icons.destroy,
        click: () => this.removeSubVolumeModal()
      }
    ];

    this.subvolumeGroup$ = this.subject.pipe(
      switchMap(() =>
        this.cephfsSubvolumeGroup.get(this.fsName).pipe(
          catchError(() => {
            this.context.error();
            return of(null);
          })
        )
      ),
      shareReplay(1)
    );
  }

  fetchData() {
    this.subject.next([]);
  }

  ngOnChanges(changes: SimpleChanges) {
    if (changes.fsName) {
      this.subject.next([]);
    }
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  openModal(edit = false) {
    this.modalService.show(
      CephfsSubvolumegroupFormComponent,
      {
        fsName: this.fsName,
        subvolumegroupName: this.selection?.first()?.name,
        pools: this.pools,
        isEdit: edit
      },
      { size: 'lg' }
    );
  }

  removeSubVolumeModal() {
    const name = this.selection.first().name;
    this.modalService.show(CriticalConfirmationModalComponent, {
      itemDescription: 'subvolume group',
      itemNames: [name],
      actionDescription: 'remove',
      submitActionObservable: () =>
        this.taskWrapper.wrapTaskAroundCall({
          task: new FinishedTask('cephfs/subvolume/group/remove', { subvolumegroupName: name }),
          call: this.cephfsSubvolumeGroup.remove(this.fsName, name)
        })
    });
  }
}
