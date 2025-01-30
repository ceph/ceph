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
import { TopicModel } from './topic.model';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { URLBuilderService } from '~/app/shared/services/url-builder.service';
import { Icons } from '~/app/shared/enum/icons.enum';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { FinishedTask } from '~/app/shared/models/finished-task';

import { Observable, Subscriber } from 'rxjs';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';

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
  topics: TopicModel[];
  errorMessage: string;
  selection: CdTableSelection = new CdTableSelection();
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
        name: $localize`Arn`,
        prop: 'arn',
        flexGrow: 2
      },
      {
        name: $localize`Push endpoint`,
        prop: 'dest.push_endpoint',
        flexGrow: 2
      }
    ];
    const addAction: CdTableAction = {
      permission: 'create',
      icon: Icons.add,
      routerLink: () => this.urlBuilder.getCreate(),
      name: this.actionLabels.CREATE,
      canBePrimary: (selection: CdTableSelection) => !selection.hasSelection
    };

    const deleteAction: CdTableAction = {
      permission: 'delete',
      icon: Icons.destroy,
      click: () => this.deleteAction(),
      disable: () => !this.selection.hasSelection,
      name: this.actionLabels.DELETE,
      canBePrimary: (selection: CdTableSelection) => selection.hasMultiSelection
    };
    this.tableActions = [addAction, deleteAction];
    this.setTableRefreshTimeout();
    this.loadTopics();
  }

  loadTopics(context?: CdTableFetchDataContext): void {
    this.setTableRefreshTimeout();
    this.rgwTopicService.listTopic().subscribe({
      next: (data) => {
        this.topics = data;
      },
      error: () => {
        if (context) {
          context.error();
        }
      }
    });
  }
  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  deleteAction() {
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
              call: this.rgwTopicService.delete(name)
            })
            .subscribe({
              error: (error: any) => {
                observer.error(error);
                this.table.refreshBtn();
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
