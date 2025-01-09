import { Component, OnInit, ViewChild } from '@angular/core';
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

@Component({
  selector: 'cd-rgw-topic-list',
  templateUrl: './rgw-topic-list.component.html',
  styleUrls: ['./rgw-topic-list.component.scss']
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
    private rgwTopicService: RgwTopicService
  ) {
    super();
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
  }

  loadTopics(context?: CdTableFetchDataContext): void {
    this.rgwTopicService.listTopic().subscribe({
      next: (data:TopicModel[]) => {
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
}
