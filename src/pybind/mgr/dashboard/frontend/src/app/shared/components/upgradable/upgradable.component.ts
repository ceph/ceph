import { Component } from '@angular/core';
import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import { Observable, Subscription } from 'rxjs';
import { UpgradeService } from '../../api/upgrade.service';
import { UpgradeInfoInterface, UpgradeStatusInterface } from '../../models/upgrade.interface';
import { OrchestratorService } from '../../api/orchestrator.service';
import { Icons } from '~/app/shared/enum/icons.enum';
import { SummaryService } from '../../services/summary.service';
import { ExecutingTask } from '../../models/executing-task';

@Component({
  selector: 'cd-upgradable',
  templateUrl: './upgradable.component.html',
  styleUrls: ['./upgradable.component.scss']
})
export class UpgradableComponent {
  orchAvailable: boolean = false;
  upgradeInfo$: Observable<UpgradeInfoInterface>;
  upgradeStatus$: Observable<UpgradeStatusInterface>;
  upgradeModalRef: NgbModalRef;
  executingTask: ExecutingTask;
  private subs = new Subscription();

  icons = Icons;

  constructor(
    private orchestratorService: OrchestratorService,
    private summaryService: SummaryService,
    private upgradeService: UpgradeService
  ) {}

  ngOnInit() {
    this.orchestratorService.status().subscribe((status: any) => {
      this.orchAvailable = status.available;
      if (this.orchAvailable && status.upgrade_status?.available) {
        this.upgradeInfo$ = this.upgradeService.listCached();
        this.upgradeStatus$ = this.upgradeService.status();
      }
    });

    this.subs.add(
      this.summaryService.subscribe((summary) => {
        this.executingTask = summary.executing_tasks.filter((tasks) =>
          tasks.name.includes('progress/Upgrade')
        )[0];
      })
    );
  }

  ngOnDestroy() {
    this.subs?.unsubscribe();
  }

  upgradeModal() {
    this.upgradeModalRef = this.upgradeService.startUpgradeModal();
  }
}
