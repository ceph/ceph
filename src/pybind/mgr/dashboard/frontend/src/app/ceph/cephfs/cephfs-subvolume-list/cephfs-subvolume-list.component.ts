import {
  Component,
  Input,
  OnChanges,
  OnInit,
  SimpleChanges,
  TemplateRef,
  ViewChild
} from '@angular/core';
import { BehaviorSubject, Observable, of } from 'rxjs';
import { catchError, switchMap, tap } from 'rxjs/operators';
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
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import { FormControl } from '@angular/forms';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CriticalConfirmationModalComponent } from '~/app/shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { CephfsSubvolumeGroupService } from '~/app/shared/api/cephfs-subvolume-group.service';
import { CephfsSubvolumeGroup } from '~/app/shared/models/cephfs-subvolume-group.model';
import { CephfsMountDetailsComponent } from '../cephfs-mount-details/cephfs-mount-details.component';
import { HealthService } from '~/app/shared/api/health.service';

@Component({
  selector: 'cd-cephfs-subvolume-list',
  templateUrl: './cephfs-subvolume-list.component.html',
  styleUrls: ['./cephfs-subvolume-list.component.scss']
})
export class CephfsSubvolumeListComponent extends CdForm implements OnInit, OnChanges {
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

  @ViewChild('removeTmpl', { static: true })
  removeTmpl: TemplateRef<any>;

  @Input() fsName: string;
  @Input() pools: any[];

  columns: CdTableColumn[] = [];
  tableActions: CdTableAction[];
  context: CdTableFetchDataContext;
  selection = new CdTableSelection();
  removeForm: CdFormGroup;
  icons = Icons;
  permissions: Permissions;
  modalRef: NgbModalRef;
  errorMessage: string = '';
  selectedName: string = '';

  subVolumes$: Observable<CephfsSubvolume[]>;
  subVolumeGroups$: Observable<CephfsSubvolumeGroup[]>;
  subject = new BehaviorSubject<CephfsSubvolume[]>([]);
  groupsSubject = new BehaviorSubject<CephfsSubvolume[]>([]);

  subvolumeGroupList: string[] = [];
  subVolumesList: CephfsSubvolume[] = [];

  activeGroupName: string = '';

  constructor(
    private cephfsSubVolumeService: CephfsSubvolumeService,
    private actionLabels: ActionLabelsI18n,
    private modalService: ModalService,
    private authStorageService: AuthStorageService,
    private taskWrapper: TaskWrapperService,
    private cephfsSubvolumeGroupService: CephfsSubvolumeGroupService,
    private healthService: HealthService
  ) {
    super();
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
        click: () => this.openModal()
      },
      {
        name: this.actionLabels.EDIT,
        permission: 'update',
        icon: Icons.edit,
        click: () => this.openModal(true)
      },
      {
        name: this.actionLabels.ATTACH,
        permission: 'read',
        icon: Icons.bars,
        disable: () => !this.selection?.hasSelection,
        click: () => this.showAttachInfo()
      },
      {
        name: this.actionLabels.REMOVE,
        permission: 'delete',
        icon: Icons.destroy,
        click: () => this.removeSubVolumeModal()
      }
    ];

    this.subVolumeGroups$ = this.groupsSubject.pipe(
      switchMap(() =>
        this.cephfsSubvolumeGroupService.get(this.fsName, false).pipe(
          tap((groups) => {
            this.subvolumeGroupList = groups.map((group) => group.name);
            this.subvolumeGroupList.unshift('');
          }),
          catchError(() => {
            this.context.error();
            return of(null);
          })
        )
      )
    );
  }

  fetchData() {
    this.subject.next([]);
  }

  ngOnChanges(changes: SimpleChanges) {
    if (changes.fsName) {
      this.subject.next([]);
      this.groupsSubject.next([]);
    }
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  showAttachInfo() {
    const selectedSubVolume = this.selection?.selected?.[0];

    this.healthService.getClusterFsid().subscribe({
      next: (clusterId: string) => {
        this.modalRef = this.modalService.show(CephfsMountDetailsComponent, {
          onSubmit: () => this.modalRef.close(),
          mountData: {
            fsId: clusterId,
            fsName: this.fsName,
            rootPath: selectedSubVolume.info.path
          }
        });
      }
    });
  }

  openModal(edit = false) {
    this.modalService.show(
      CephfsSubvolumeFormComponent,
      {
        fsName: this.fsName,
        subVolumeName: this.selection?.first()?.name,
        subVolumeGroupName: this.activeGroupName,
        pools: this.pools,
        isEdit: edit
      },
      { size: 'lg' }
    );
  }

  removeSubVolumeModal() {
    this.removeForm = new CdFormGroup({
      retainSnapshots: new FormControl(false)
    });
    this.errorMessage = '';
    this.selectedName = this.selection.first().name;
    this.modalRef = this.modalService.show(CriticalConfirmationModalComponent, {
      actionDescription: 'Remove',
      itemNames: [this.selectedName],
      itemDescription: 'Subvolume',
      childFormGroup: this.removeForm,
      childFormGroupTemplate: this.removeTmpl,
      submitAction: () =>
        this.taskWrapper
          .wrapTaskAroundCall({
            task: new FinishedTask('cephfs/subvolume/remove', { subVolumeName: this.selectedName }),
            call: this.cephfsSubVolumeService.remove(
              this.fsName,
              this.selectedName,
              this.activeGroupName,
              this.removeForm.getValue('retainSnapshots')
            )
          })
          .subscribe({
            complete: () => this.modalRef.close(),
            error: (error) => {
              this.modalRef.componentInstance.stopLoadingSpinner();
              this.errorMessage = error.error.detail;
            }
          })
    });
  }

  selectSubVolumeGroup(subVolumeGroupName: string) {
    this.activeGroupName = subVolumeGroupName;
    this.getSubVolumes();
  }

  getSubVolumes() {
    this.subVolumes$ = this.subject.pipe(
      switchMap(() =>
        this.cephfsSubVolumeService.get(this.fsName, this.activeGroupName).pipe(
          catchError(() => {
            this.context?.error();
            return of(null);
          })
        )
      )
    );
  }
}
