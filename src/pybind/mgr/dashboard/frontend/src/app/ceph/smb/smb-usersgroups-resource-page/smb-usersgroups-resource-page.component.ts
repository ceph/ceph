import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { Subscription } from 'rxjs';
import { SmbService } from '~/app/shared/api/smb.service';
import { OverviewField } from '~/app/shared/components/resource-overview-card/resource-overview-card.component';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { SMBUsersGroups } from '../smb.model';

@Component({
  selector: 'cd-smb-usersgroups-resource-page',
  templateUrl: './smb-usersgroups-resource-page.component.html',
  styleUrls: ['./smb-usersgroups-resource-page.component.scss'],
  standalone: false
})
export class SmbUsersgroupsResourcePageComponent implements OnInit, OnDestroy {
  private sub = new Subscription();

  section = '';
  selection: SMBUsersGroups;
  notFound = false;
  overviewFields: OverviewField[] = [];
  columns: CdTableColumn[] = [];

  constructor(
    private route: ActivatedRoute,
    private smbService: SmbService
  ) {}

  ngOnInit(): void {
    this.section = this.route.snapshot.data['section'] ?? 'overview';
    const usersGroupsIdParamMap = this.route.parent
      ? this.route.parent.paramMap
      : this.route.paramMap;
    this.columns = [
      {
        name: $localize`Username`,
        prop: 'name',
        flexGrow: 2
      }
    ];

    this.sub.add(
      usersGroupsIdParamMap.subscribe((params) => {
        const usersGroupsId = decodeURIComponent(params.get('users_groups_id') ?? '');
        if (!usersGroupsId) {
          this.applyStandalone(null);
          return;
        }

        this.sub.add(
          this.smbService.getUsersGroups(usersGroupsId).subscribe({
            next: (standalone: SMBUsersGroups) => this.applyStandalone(standalone),
            error: () => this.applyStandalone(null)
          })
        );
      })
    );
  }

  ngOnDestroy(): void {
    this.sub.unsubscribe();
  }

  private applyStandalone(standalone: SMBUsersGroups | null): void {
    this.notFound = !standalone;
    if (!standalone) {
      this.selection = undefined;
      this.overviewFields = [];
      return;
    }

    this.selection = standalone;
    this.overviewFields = this.buildOverviewFields(standalone);
  }

  private buildOverviewFields(standalone: SMBUsersGroups): OverviewField[] {
    return [
      { label: $localize`Name`, value: standalone.users_groups_id },
      { label: $localize`Number of users`, value: standalone.values?.users?.length || 0 },
      {
        label: $localize`Groups`,
        values: (standalone.values?.groups || []).map((group) => group.name),
        type: 'tags'
      },
      {
        label: $localize`Linked to cluster`,
        value: standalone.linked_to_cluster
      }
    ];
  }
}
