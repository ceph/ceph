import {
  Component,
  EventEmitter,
  OnInit,
  Output,
  Input,
  ViewChild,
  TemplateRef,
  OnDestroy
} from '@angular/core';
import { Subscription } from 'rxjs';
import { mergeMap } from 'rxjs/operators';

import { CdForm } from '~/app/shared/forms/cd-form';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Host } from '~/app/shared/models/host.interface';
import { HostStatus } from '~/app/shared/enum/host-status.enum';
import { HostService } from '~/app/shared/api/host.service';
import { OrchestratorService } from '~/app/shared/api/orchestrator.service';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { CephServiceService } from '~/app/shared/api/ceph-service.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CephServiceSpec } from '~/app/shared/models/service.interface';
import { TaskMessageService } from '~/app/shared/services/task-message.service';

@Component({
  selector: 'cd-nvmeof-gateway-node-add-modal',
  templateUrl: './nvmeof-gateway-node-add-modal.component.html',
  styleUrls: ['./nvmeof-gateway-node-add-modal.component.scss'],
  standalone: false
})
export class NvmeofGatewayNodeAddModalComponent extends CdForm implements OnInit, OnDestroy {
  @Input()
  usedHostnames: string[] = [];

  @Input()
  groupName!: string;

  @Input()
  serviceSpec!: CephServiceSpec;

  @Output()
  gatewayAdded = new EventEmitter<void>();

  hosts: Host[] = [];
  columns: CdTableColumn[] = [];
  selection = new CdTableSelection();
  isLoadingHosts = false;
  private tableContext: CdTableFetchDataContext = null;
  private sub = new Subscription();

  @ViewChild('statusTemplate', { static: true })
  statusTemplate!: TemplateRef<any>;

  @ViewChild('labelsTemplate', { static: true })
  labelsTemplate!: TemplateRef<any>;

  @ViewChild('addrTemplate', { static: true })
  addrTemplate!: TemplateRef<any>;

  HostStatus = HostStatus;

  constructor(
    private hostService: HostService,
    private orchService: OrchestratorService,
    private cephServiceService: CephServiceService,
    private notificationService: NotificationService,
    private taskMessageService: TaskMessageService
  ) {
    super();
  }

  ngOnInit(): void {
    this.columns = [
      {
        name: $localize`Hostname`,
        prop: 'hostname',
        flexGrow: 2
      },
      {
        name: $localize`IP address`,
        prop: 'addr',
        flexGrow: 2,
        cellTemplate: this.addrTemplate
      },
      {
        name: $localize`Status`,
        prop: 'status',
        flexGrow: 1,
        cellTemplate: this.statusTemplate
      },
      {
        name: $localize`Labels (tags)`,
        prop: 'labels',
        flexGrow: 3,
        cellTemplate: this.labelsTemplate
      }
    ];
  }

  ngOnDestroy(): void {
    this.sub.unsubscribe();
  }

  getHosts(context: CdTableFetchDataContext) {
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

    this.sub.add(
      this.orchService
        .status()
        .pipe(
          mergeMap((orchStatus) => {
            const factsAvailable = this.hostService.checkHostsFactsAvailable(orchStatus);
            return this.hostService.list(this.tableContext?.toParams(), factsAvailable.toString());
          })
        )
        .subscribe({
          next: (hostList: Host[]) => {
            this.hosts = hostList
              .map((host: Host) => ({
                ...host,
                status: host.status || HostStatus.AVAILABLE
              }))
              .filter((host: Host) => {
                return !this.usedHostnames.includes(host.hostname);
              });

            this.isLoadingHosts = false;
          },
          error: () => {
            this.isLoadingHosts = false;
            context.error();
          }
        })
    );
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  onSubmit() {
    if (!this.serviceSpec) {
      this.notificationService.show(
        NotificationType.error,
        $localize`Service specification is missing.`
      );
      return;
    }

    this.loadingStart();

    // 1. Prepare Host Lists
    const selectedHosts = this.selection.selected.map((h: Host) => h.hostname);
    const currentHosts = this.serviceSpec.placement?.hosts || [];
    const newHosts = [...currentHosts, ...selectedHosts];

    // 2. Construct Payload (Safe Copy)
    const modifiedSpec: any = { ...this.serviceSpec };
    // Remove read-only fields that cause backend errors
    delete modifiedSpec.status;
    delete modifiedSpec.events;

    // Deep clone placement to safely modify it
    if (modifiedSpec.placement) {
      modifiedSpec.placement = { ...modifiedSpec.placement };
    } else {
      modifiedSpec.placement = {};
    }

    // Assign the NEW list of hosts
    modifiedSpec.placement.hosts = newHosts;

    // 3. Send Update Request
    this.cephServiceService.update(modifiedSpec).subscribe({
      next: () => {
        this.notificationService.show(
          NotificationType.success,
          this.taskMessageService.messages['nvmeof/gateway/node/add'].success({
            group_name: this.groupName
          })
        );
        this.gatewayAdded.emit(); // Refresh parent
        this.loadingReady();
        this.closeModal(); // Close modal
      },
      error: (e) => {
        this.loadingReady();
        this.notificationService.show(
          NotificationType.error,
          this.taskMessageService.messages['nvmeof/gateway/node/add'].failure({
            group_name: this.groupName
          }),
          e
        );
      }
    });
  }
}
