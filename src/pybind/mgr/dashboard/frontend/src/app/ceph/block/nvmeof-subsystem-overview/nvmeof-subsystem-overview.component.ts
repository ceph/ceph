import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute, NavigationEnd, Router } from '@angular/router';
import { forkJoin, Subscription } from 'rxjs';
import { filter } from 'rxjs/operators';

import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import {
  NvmeofSubsystem,
  NO_AUTH,
  getSubsystemAuthStatus,
  isSubsystemAllowAllHosts,
  normalizeInitiators
} from '~/app/shared/models/nvmeof';
import { URLVerbs } from '~/app/shared/constants/app.constants';
import { ICON_TYPE } from '~/app/shared/enum/icons.enum';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { NvmeofEditAuthenticationComponent } from '../nvmeof-edit-authentication/nvmeof-edit-authentication.component';

@Component({
  selector: 'cd-nvmeof-subsystem-overview',
  templateUrl: './nvmeof-subsystem-overview.component.html',
  styleUrls: ['./nvmeof-subsystem-overview.component.scss'],
  standalone: false
})
export class NvmeofSubsystemOverviewComponent implements OnInit, OnDestroy {
  subsystemNQN: string;
  groupName: string;
  subsystem: NvmeofSubsystem;
  authStatus = NO_AUTH;
  allowAllHosts = false;
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
      const initiatorList = normalizeInitiators(initiators);
      this.authStatus = getSubsystemAuthStatus(this.subsystem, initiatorList);
      this.allowAllHosts = isSubsystemAllowAllHosts(this.subsystem, initiatorList);
    });
  }

  /** Host access is configuration text, not a health state. */
  getHostAccessLabel(allowAllHosts: boolean): string {
    return allowAllHosts ? $localize`Allow all hosts` : $localize`Restrict to specific hosts`;
  }

  getAuthStatusIcon(authStatus: string): keyof typeof ICON_TYPE {
    return authStatus === NO_AUTH ? 'error' : 'success';
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
