import { Component, NgZone, OnInit, ViewChild } from '@angular/core';
import _ from 'lodash';

import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { ListWithDetails } from '~/app/shared/classes/list-with-details.class';
import { Permission } from '~/app/shared/models/permissions';

import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { RgwTopicService } from '~/app/shared/api/rgw-topic.service';

import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { URLBuilderService } from '~/app/shared/services/url-builder.service';
import { Icons } from '~/app/shared/enum/icons.enum';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { Topic } from '~/app/shared/models/topic.model';
import { BehaviorSubject, Observable, of, Subscriber } from 'rxjs';
import { catchError, shareReplay, switchMap } from 'rxjs/operators';

const BASE_URL = 'rgw/topic';
@Component({
  selector: 'cd-rgw-topic-list',
  templateUrl: './rgw-topic-list.component.html',
  styleUrls: ['./rgw-topic-list.component.scss'],
  providers: [{ provide: URLBuilderService, useValue: new URLBuilderService(BASE_URL) }]
})
export class RgwTopicListComponent extends ListWithDetails implements OnInit {
  @ViewChild('table', { static: true })
  table: TableComponent;
  columns: CdTableColumn[];
  permission: Permission;
  tableActions: CdTableAction[];
  context: CdTableFetchDataContext;
  errorMessage: string;
  selection: CdTableSelection = new CdTableSelection();
  topicsSubject = new BehaviorSubject<Topic[]>([]);
  topics$ = this.topicsSubject.asObservable();
  name: string;
  constructor(
    private authStorageService: AuthStorageService,
    public actionLabels: ActionLabelsI18n,
    private rgwTopicService: RgwTopicService,
    private modalService: ModalCdsService,
    private urlBuilder: URLBuilderService,
    private taskWrapper: TaskWrapperService,
    protected ngZone: NgZone
  ) {
    super(ngZone);
    this.permission = this.authStorageService.getPermissions().rgw;
  }

  ngOnInit() {
    this.columns = [
      {
        name: $localize`Name`,
        prop: 'name',
        flexGrow: 2
      },
      {
        name: $localize`Owner`,
        prop: 'owner',
        flexGrow: 2
      },
      {
        name: $localize`Amazon resource name`,
        prop: 'arn',
        flexGrow: 2
      },
      {
        name: $localize`Push endpoint`,
        prop: 'dest.push_endpoint',
        flexGrow: 2
      }
    ];

    const getBucketUri = () =>
      this.selection.first() && `${encodeURIComponent(this.selection.first().key)}`;
    const addAction: CdTableAction = {
      permission: 'create',
      icon: Icons.add,
      routerLink: () => this.urlBuilder.getCreate(),
      name: this.actionLabels.CREATE,
      canBePrimary: (selection: CdTableSelection) => !selection.hasSelection
    };

    const editAction: CdTableAction = {
      permission: 'update',
      icon: Icons.edit,
      routerLink: () => this.urlBuilder.getEdit(getBucketUri()),
      name: this.actionLabels.EDIT
    };

    const deleteAction: CdTableAction = {
      permission: 'delete',
      icon: Icons.destroy,
      click: () => this.deleteAction(),
      disable: () => !this.selection.hasSelection,
      name: this.actionLabels.DELETE,
      canBePrimary: (selection: CdTableSelection) => !selection.hasSelection
    };
    this.tableActions = [addAction, editAction, deleteAction];
    this.setTableRefreshTimeout();
    this.topics$ = this.topicsSubject.pipe(
      switchMap(() =>
        this.rgwTopicService.listTopic().pipe(
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
    this.topicsSubject.next([]);
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  deleteAction() {
    const key = this.selection.first().key;
    const name = this.selection.first().name;
    this.modalService.show(DeleteConfirmationModalComponent, {
      itemDescription: $localize`Topic`,
      itemNames: [name],
      submitActionObservable: () => {
        return new Observable((observer: Subscriber<any>) => {
          this.taskWrapper
            .wrapTaskAroundCall({
              task: new FinishedTask('rgw/topic/delete', {
                name: [name]
              }),
              call: this.rgwTopicService.delete(key)
            })
            .subscribe({
              error: (error: any) => {
                observer.error(error);
              },
              complete: () => {
                observer.complete();
                this.table.refreshBtn();
              }
            });
        });
      }
    });
  }
}
