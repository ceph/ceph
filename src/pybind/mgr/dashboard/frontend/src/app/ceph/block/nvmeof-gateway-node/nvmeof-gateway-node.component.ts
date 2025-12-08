import { Component, Host, OnDestroy, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { Subscription } from 'rxjs';
import { mergeMap } from 'rxjs/operators';

import { HostService } from '~/app/shared/api/host.service';
import { OrchestratorService } from '~/app/shared/api/orchestrator.service';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { OrchestratorStatus } from '~/app/shared/models/orchestrator.interface';
import { Permission } from '~/app/shared/models/permissions';

import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { CdTableServerSideService } from '~/app/shared/services/cd-table-server-side.service';

import _ from 'lodash';

@Component({
  selector: 'cd-nvmeof-gateway-node',
  templateUrl: './nvmeof-gateway-node.component.html',
  styleUrls: ['./nvmeof-gateway-node.component.scss']
})
export class NvmeofGatewayNodeComponent implements OnInit, OnDestroy {
  @ViewChild(TableComponent, { static: true })
  table: TableComponent;

  @ViewChild('hostNameTpl', { static: true })
  hostNameTpl: TemplateRef<any>;
  @ViewChild('orchTmpl', { static: true })
  orchTmpl: TemplateRef<any>;

  permission: Permission;
  columns: CdTableColumn[] = [];
  hosts: Host[] = [];
  isLoadingHosts = false;
  tableActions: CdTableAction[];
  selection = new CdTableSelection();
  icons = Icons;
  private tableContext: CdTableFetchDataContext = null;
  count = 5;
  orchStatus: OrchestratorStatus;
  private sub = new Subscription();

  constructor(
    private authStorageService: AuthStorageService,
    private hostService: HostService,
    private orchService: OrchestratorService
  ) {
    this.permission = this.authStorageService.getPermissions().nvmeof;
  }

  ngOnInit(): void {
    this.columns = [
      {
        name: $localize`Hostname`,
        prop: 'hostname',
        flexGrow: 1,
        cellTemplate: this.hostNameTpl
      },

      {
        name: $localize`Status`,
        prop: 'status',
        flexGrow: 0.8,
        cellTransformation: CellTemplate.tag,
        customTemplateConfig: {
          map: {
            maintenance: { class: 'tag-warning' },
            available: { class: 'tag-success' }
          }
        }
      }
    ];
  }

  ngOnDestroy(): void {
    this.sub.unsubscribe();
  }

  updateSelection(selection: CdTableSelection): void {
    this.selection = selection;
  }

  getSelectedHosts(): any[] {
    return this.selection.selected;
  }

  getSelectedHostnames(): string[] {
    return this.selection.selected.map((host: any) => host.hostname);
  }

  checkHostsFactsAvailable(): boolean {
    const orchFeatures = this.orchStatus?.features;
    if (!_.isEmpty(orchFeatures)) {
      if (orchFeatures.get_facts?.available) {
        return true;
      }
      return false;
    }
    return false;
  }

  getHosts(context: CdTableFetchDataContext): void {
    if (context !== null) {
      this.tableContext = context;
    }
    if (this.tableContext == null) {
      this.tableContext = new CdTableFetchDataContext(() => undefined);
    }
    if (this.isLoadingHosts) {
      return;
    }
    this.isLoadingHosts = true;
    this.sub = this.orchService
      .status()
      .pipe(
        mergeMap((orchStatus) => {
          this.orchStatus = orchStatus;
          const factsAvailable = this.checkHostsFactsAvailable();
          return this.hostService.list(this.tableContext?.toParams(), factsAvailable.toString());
        })
      )
      .subscribe(
        (hostList: any[]) => {
          this.hosts = hostList;
          this.hosts.forEach((host: any) => {
            if (host['status'] === '') {
              host['status'] = 'available';
            }
          });
          this.isLoadingHosts = false;
          if (this.hosts.length > 0) {
            this.count = CdTableServerSideService.getCount(hostList[0]);
          } else {
            this.count = 0;
          }
        },
        () => {
          this.isLoadingHosts = false;
          context.error();
        }
      );
  }
}
