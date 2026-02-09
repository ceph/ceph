import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { forkJoin, of } from 'rxjs';
import { catchError, map, switchMap } from 'rxjs/operators';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import {
  NvmeofSubsystem,
  NvmeofSubsystemData,
  NvmeofSubsystemInitiator,
  getSubsystemAuthStatus
} from '~/app/shared/models/nvmeof';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';

import { ICON_TYPE } from '~/app/shared/enum/icons.enum';
import { NvmeofSubsystemAuthType } from '~/app/shared/enum/nvmeof.enum';

@Component({
  selector: 'cd-nvmeof-gateway-subsystem',
  templateUrl: './nvmeof-gateway-subsystem.component.html',
  styleUrls: ['./nvmeof-gateway-subsystem.component.scss'],
  standalone: false
})
export class NvmeofGatewaySubsystemComponent implements OnInit {
  @ViewChild('authTpl', { static: true })
  authTpl!: TemplateRef<any>;

  groupName!: string;

  columns: CdTableColumn[] = [];

  subsystems: NvmeofSubsystemData[] = [];
  selection = new CdTableSelection();

  iconType = ICON_TYPE;
  authType = NvmeofSubsystemAuthType;

  constructor(private nvmeofService: NvmeofService, private route: ActivatedRoute) {}

  ngOnInit(): void {
    this.columns = [
      {
        name: $localize`Subsystem NQN`,
        prop: 'nqn',
        flexGrow: 2
      },
      {
        name: $localize`Authentication`,
        prop: 'auth',
        flexGrow: 1.5,
        cellTemplate: this.authTpl
      },
      {
        name: $localize`Hosts (Initiators)`,
        prop: 'hosts',
        flexGrow: 1
      }
    ];

    this.route.parent?.params.subscribe((params) => {
      if (params['group']) {
        this.groupName = params['group'];
        this.getSubsystemsData();
      }
    });
  }

  getSubsystemsData() {
    this.nvmeofService
      .listSubsystems(this.groupName)
      .pipe(
        switchMap((subsystems: NvmeofSubsystem[] | NvmeofSubsystem) => {
          const subs = Array.isArray(subsystems) ? subsystems : [subsystems];
          if (subs.length === 0) return of([]);

          return forkJoin(
            subs.map((sub) =>
              this.nvmeofService.getInitiators(sub.nqn, this.groupName).pipe(
                catchError(() => of([])),
                map(
                  (
                    initiators: NvmeofSubsystemInitiator[] | { hosts?: NvmeofSubsystemInitiator[] }
                  ) => {
                    let count = 0;
                    if (Array.isArray(initiators)) count = initiators.length;
                    else if (initiators?.hosts && Array.isArray(initiators.hosts)) {
                      count = initiators.hosts.length;
                    }

                    return {
                      ...sub,
                      auth: getSubsystemAuthStatus(sub, initiators),
                      hosts: count
                    };
                  }
                )
              )
            )
          );
        })
      )
      .subscribe({
        next: (subsystems: NvmeofSubsystemData[]) => {
          this.subsystems = subsystems;
        },
        error: () => {
          this.subsystems = [];
        }
      });
  }

  updateSelection(selection: CdTableSelection): void {
    this.selection = selection;
  }
}
