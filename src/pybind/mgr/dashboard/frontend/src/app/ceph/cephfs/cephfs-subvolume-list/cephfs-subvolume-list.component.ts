import { Component, Input, OnChanges, OnInit, ViewChild } from '@angular/core';
import { Observable, ReplaySubject, of } from 'rxjs';
import { catchError, shareReplay, switchMap } from 'rxjs/operators';
import { CephfsSubvolumeService } from '~/app/shared/api/cephfs-subvolume.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { CephfsSubvolume } from '~/app/shared/models/cephfs-subvolume.model';
import { ModalService } from '~/app/shared/services/modal.service';
import { CephfsSubvolumeFormComponent } from '../cephfs-subvolume-form/cephfs-subvolume-form.component';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { Permissions } from '~/app/shared/models/permissions';
import { CriticalConfirmationModalComponent } from '~/app/shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { FinishedTask } from '~/app/shared/models/finished-task';

@Component({
  selector: 'cd-cephfs-subvolume-list',
  templateUrl: './cephfs-subvolume-list.component.html',
  styleUrls: ['./cephfs-subvolume-list.component.scss']
})
export class CephfsSubvolumeListComponent implements OnInit, OnChanges {
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

  @Input() fsName: string;
  @Input() pools: any[];

  columns: CdTableColumn[] = [];
  tableActions: CdTableAction[];
  context: CdTableFetchDataContext;
  selection = new CdTableSelection();
  icons = Icons;
  permissions: Permissions;

  subVolumes$: Observable<CephfsSubvolume[]>;
  subject = new ReplaySubject<CephfsSubvolume[]>();

  constructor(
    private cephfsSubVolume: CephfsSubvolumeService,
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
        flexGrow: 1,
        cellTemplate: this.nameTpl
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
        name: $localize`Path`,
        prop: 'info.path',
        flexGrow: 1,
        cellTransformation: CellTemplate.path
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
        click: () =>
          this.modalService.show(
            CephfsSubvolumeFormComponent,
            {
              fsName: this.fsName,
              pools: this.pools
            },
            { size: 'lg' }
          )
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

    this.subVolumes$ = this.subject.pipe(
      switchMap(() =>
        this.cephfsSubVolume.get(this.fsName).pipe(
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
    this.subject.next();
  }

  ngOnChanges() {
    this.subject.next();
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  openModal(edit = false) {
    this.modalService.show(
      CephfsSubvolumeFormComponent,
      {
        fsName: this.fsName,
        subVolumeName: this.selection?.first()?.name,
        pools: this.pools,
        isEdit: edit
      },
      { size: 'lg' }
    );
  }

  removeSubVolumeModal() {
    const name = this.selection.first().name;
    this.modalService.show(CriticalConfirmationModalComponent, {
      itemDescription: 'subvolume',
      itemNames: [name],
      actionDescription: 'remove',
      submitActionObservable: () =>
        this.taskWrapper.wrapTaskAroundCall({
          task: new FinishedTask('cephfs/subvolume/remove', { subVolumeName: name }),
          call: this.cephfsSubVolume.remove(this.fsName, name)
        })
    });
  }
}
