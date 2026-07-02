import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute, NavigationEnd, Router } from '@angular/router';
import { forkJoin, Subscription } from 'rxjs';
import { filter } from 'rxjs/operators';

import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import {
  NvmeofSubsystem,
  NvmeofSubsystemInitiator,
  NO_AUTH,
  getSubsystemAuthStatus
} from '~/app/shared/models/nvmeof';
import { URLVerbs } from '~/app/shared/constants/app.constants';
import { ICON_TYPE } from '~/app/shared/enum/icons.enum';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { NvmeofEditAuthenticationComponent } from '../nvmeof-edit-authentication/nvmeof-edit-authentication.component';

export interface SubsystemDetail {
  label: string;
  value: string | number | boolean;
  type: 'text' | 'host-access' | 'auth' | 'listeners';
  tooltip?: string;
  row: number;
}

@Component({
  selector: 'cd-nvmeof-subsystem-overview',
  templateUrl: './nvmeof-subsystem-overview.component.html',
  styleUrls: ['./nvmeof-subsystem-overview.component.scss'],
  standalone: false
})
export class NvmeofSubsystemOverviewComponent implements OnInit, OnDestroy {
  subsystemNQN!: string;
  groupName!: string;
  subsystem!: NvmeofSubsystem;
  details: SubsystemDetail[] = [];
  private subscriptions = new Subscription();

  constructor(
    private route: ActivatedRoute,
    private router: Router,
    private nvmeofService: NvmeofService,
    private modalService: ModalCdsService
  ) {}

  ngOnInit() {
    this.subscriptions.add(
      this.route.parent?.params.subscribe((params) => {
        this.subsystemNQN = params['subsystem_nqn'];
        this.fetchIfReady();
      })
    );
    this.subscriptions.add(
      this.route.queryParams.subscribe((qp) => {
        this.groupName = qp['group'];
        this.fetchIfReady();
      })
    );
    this.subscriptions.add(
      this.router.events
        .pipe(
          filter(
            (event): event is NavigationEnd =>
              event instanceof NavigationEnd && !event.urlAfterRedirects.includes('(modal:')
          )
        )
        .subscribe(() => {
          this.fetchIfReady();
        })
    );
  }

  ngOnDestroy() {
    this.subscriptions.unsubscribe();
  }

  private fetchIfReady() {
    if (this.subsystemNQN && this.groupName) {
      this.fetchSubsystem();
    }
  }

  fetchSubsystem() {
    forkJoin({
      subsystem: this.nvmeofService.getSubsystem(this.subsystemNQN, this.groupName),
      initiators: this.nvmeofService.getInitiators(this.subsystemNQN, this.groupName)
    }).subscribe(({ subsystem, initiators }) => {
      this.subsystem = subsystem as NvmeofSubsystem;
      const initiatorList = initiators as
        | NvmeofSubsystemInitiator[]
        | { hosts?: NvmeofSubsystemInitiator[] };
      this.buildDetails(getSubsystemAuthStatus(this.subsystem, initiatorList));
    });
  }

  private buildDetails(authStatus: string) {
    this.details = [
      {
        label: $localize`Serial number`,
        value: this.subsystem.serial_number,
        type: 'text',
        row: 1
      },
      { label: $localize`Model Number`, value: this.subsystem.model_number, type: 'text', row: 1 },
      {
        label: $localize`Gateway group`,
        value: this.subsystem.gw_group || this.groupName,
        type: 'text',
        row: 1
      },
      {
        label: $localize`Subsystem Type`,
        value: this.subsystem.subtype,
        type: 'text',
        row: 2
      },
      {
        label: $localize`Host access`,
        value: this.subsystem.allow_any_host ?? false,
        type: 'host-access',
        row: 2
      },
      {
        label: $localize`Authentication`,
        value: authStatus,
        type: 'auth',
        row: 2
      },
      {
        label: $localize`Listeners`,
        value:
          (this.subsystem.network_mask?.length ?? 0) > 0
            ? $localize`Auto-fetched`
            : $localize`Manually selected`,
        type: 'listeners',
        tooltip: $localize`Listeners are automatically fetched from the gateway`,
        row: 3
      },
      {
        label: $localize`Maximum Controller Identifier`,
        value: this.subsystem.max_cntlid,
        type: 'text',
        row: 3
      },
      {
        label: $localize`Minimum Controller Identifier`,
        value: this.subsystem.min_cntlid,
        type: 'text',
        row: 3
      },
      { label: $localize`Namespaces`, value: this.subsystem.namespace_count, type: 'text', row: 4 },
      {
        label: $localize`Maximum allowed namespaces`,
        value: this.subsystem.max_namespaces,
        type: 'text',
        row: 4
      }
    ];
  }

  getRows(): number[] {
    return [...new Set(this.details.map((d) => d.row))];
  }

  getDetailsForRow(row: number): SubsystemDetail[] {
    return this.details.filter((d) => d.row === row);
  }

  getDisplayValue(value: string | number | boolean): string {
    if (typeof value === 'boolean') {
      return value ? $localize`Enabled` : $localize`Disabled`;
    }
    return String(value);
  }

  getAuthStatusIcon(authStatus: string): keyof typeof ICON_TYPE {
    return authStatus === NO_AUTH ? 'error' : 'success';
  }

  getStatusIcon(detail: SubsystemDetail): keyof typeof ICON_TYPE {
    return detail.value ? 'success' : 'error';
  }

  getColNumbers(detail: SubsystemDetail): { sm: number; md: number; lg: number } {
    return detail.type === 'auth' ? { sm: 4, md: 8, lg: 12 } : { sm: 4, md: 4, lg: 4 };
  }

  isFullWidthRow(row: number): boolean {
    return this.getDetailsForRow(row).some((d) => d.type === 'auth');
  }

  getFillerCount(row: number): number[] {
    const needed = 3 - this.getDetailsForRow(row).length;
    return Array.from({ length: needed });
  }

  openEditAuthModal() {
    const modalRef = this.modalService.show(NvmeofEditAuthenticationComponent, {
      subsystemNQN: this.subsystemNQN,
      groupName: this.groupName
    });
    if (modalRef?.closeChange) {
      modalRef.closeChange.subscribe(() => this.fetchSubsystem());
    }
  }

  openEditHostAccessModal() {
    this.router.navigate([{ outlets: { modal: [URLVerbs.ADD, 'initiator'] } }], {
      queryParams: { group: this.groupName },
      relativeTo: this.route.parent
    });
  }
}
