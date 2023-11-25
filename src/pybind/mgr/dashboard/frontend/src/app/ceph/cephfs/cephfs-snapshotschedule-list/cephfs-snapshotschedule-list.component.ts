import { Component, Input, OnChanges, OnInit, SimpleChanges, ViewChild } from '@angular/core';
import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import { BehaviorSubject, Observable } from 'rxjs';
import { finalize, shareReplay, switchMap } from 'rxjs/operators';
import { CephfsSnapshotScheduleService } from '~/app/shared/api/cephfs-snapshot-schedule.service';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Permissions } from '~/app/shared/models/permissions';
import { SnapshotSchedule } from '~/app/shared/models/snapshot-schedule';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalService } from '~/app/shared/services/modal.service';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';

@Component({
  selector: 'cd-cephfs-snapshotschedule-list',
  templateUrl: './cephfs-snapshotschedule-list.component.html',
  styleUrls: ['./cephfs-snapshotschedule-list.component.scss']
})
export class CephfsSnapshotscheduleListComponent extends CdForm implements OnInit, OnChanges {
  @Input() fsName!: string;

  @ViewChild('pathTpl', { static: true })
  pathTpl: any;

  snapshotSchedules$!: Observable<SnapshotSchedule[]>;
  subject$ = new BehaviorSubject<SnapshotSchedule[]>([]);
  isLoading$ = new BehaviorSubject<boolean>(true);
  columns: CdTableColumn[] = [];
  tableActions: CdTableAction[] = [];
  context!: CdTableFetchDataContext;
  selection = new CdTableSelection();
  permissions!: Permissions;
  modalRef!: NgbModalRef;
  errorMessage: string = '';
  selectedName: string = '';
  icons = Icons;

  constructor(
    private snapshotScheduleService: CephfsSnapshotScheduleService,
    private authStorageService: AuthStorageService,
    private modalService: ModalService
  ) {
    super();
    this.permissions = this.authStorageService.getPermissions();
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes.fsName) {
      this.subject$.next([]);
    }
  }

  ngOnInit(): void {
    this.snapshotSchedules$ = this.subject$.pipe(
      switchMap(() =>
        this.snapshotScheduleService
          .getSnapshotScheduleList('/', this.fsName)
          .pipe(finalize(() => this.isLoading$.next(false)))
      ),
      shareReplay(1)
    );

    this.columns = [
      { prop: 'path', name: $localize`Path`, flexGrow: 3, cellTemplate: this.pathTpl },
      { prop: 'subvol', name: $localize`Subvolume` },
      { prop: 'schedule', name: $localize`Repeat interval` },
      { prop: 'retention', name: $localize`Retention policy` },
      { prop: 'created_count', name: $localize`Created Count` },
      { prop: 'pruned_count', name: $localize`Deleted Count` },
      { prop: 'start', name: $localize`Start time`, cellTransformation: CellTemplate.timeAgo },
      { prop: 'created', name: $localize`Created`, cellTransformation: CellTemplate.timeAgo }
    ];

    this.tableActions = [];
  }

  fetchData() {
    this.subject$.next([]);
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  openModal(edit = false) {
    this.modalService.show(
      {},
      {
        fsName: 'fs1',
        isEdit: edit
      },
      { size: 'lg' }
    );
  }
}
