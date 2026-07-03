import { Injectable, OnDestroy } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { NvmeofService, GroupsComboboxItem } from '~/app/shared/api/nvmeof.service';
import { CephServiceSpec } from '~/app/shared/models/service.interface';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';

const DEFAULT_PLACEHOLDER = $localize`Enter group name`;

@Injectable()
export class GatewayGroupQueryHandlerService implements OnDestroy {
  group: string = null;
  gwGroups: GroupsComboboxItem[] = [];
  groupSelectionCleared = false;
  gwGroupsEmpty = false;
  gwGroupPlaceholder: string = DEFAULT_PLACEHOLDER;

  dataRefresh$ = new Subject<void>();
  private destroy$ = new Subject<void>();

  constructor(
    private route: ActivatedRoute,
    private router: Router,
    private nvmeofService: NvmeofService
  ) {}

  init() {
    this.route.queryParams.pipe(takeUntil(this.destroy$)).subscribe((params) => {
      const group = params?.['group']?.trim() || null;
      const hasGroupParam = Object.prototype.hasOwnProperty.call(params ?? {}, 'group');
      if (group) {
        if (this.group === group && !this.groupSelectionCleared) {
          return;
        }
        this.groupSelectionCleared = false;
        this.onGroupSelection({ content: group }, false);
      } else if (hasGroupParam) {
        if (!this.group && this.groupSelectionCleared) {
          return;
        }
        this.groupSelectionCleared = true;
        if (this.group) {
          this.onGroupClear(false);
        } else {
          this.dataRefresh$.next();
        }
      } else {
        if (!this.group && !this.groupSelectionCleared) {
          return;
        }
        this.groupSelectionCleared = false;
        this.group = null;
      }
    });
    this.fetchGatewayGroups();
  }

  onGroupChange(group: string | null): void {
    if (group) {
      this.onGroupSelection({ content: group });
    } else {
      this.onGroupClear();
    }
  }

  onGroupSelection(selected: GroupsComboboxItem, syncQueryParam = true) {
    selected.selected = true;
    this.group = selected.content;
    this.groupSelectionCleared = false;
    if (syncQueryParam) {
      this.syncGroupQueryParam(this.group);
    }
    this.dataRefresh$.next();
  }

  onGroupClear(syncQueryParam = true) {
    this.group = null;
    this.groupSelectionCleared = true;
    if (syncQueryParam) {
      this.syncGroupQueryParam(null);
    }
    this.dataRefresh$.next();
  }

  private syncGroupQueryParam(group: string | null): void {
    const currentGroup = this.route.snapshot.queryParams['group']?.trim() || null;
    if (currentGroup === group) {
      return;
    }
    this.router.navigate([], {
      relativeTo: this.route,
      queryParams: { group: group || null },
      queryParamsHandling: 'merge',
      replaceUrl: true
    });
  }

  private fetchGatewayGroups() {
    this.nvmeofService
      .listGatewayGroups()
      .pipe(takeUntil(this.destroy$))
      .subscribe({
        next: (response: CephServiceSpec[][]) => this.handleGatewayGroupsSuccess(response),
        error: (error) => this.handleGatewayGroupsError(error)
      });
  }

  private handleGatewayGroupsSuccess(response: CephServiceSpec[][]) {
    if (response?.[0]?.length) {
      this.gwGroups = this.nvmeofService.formatGwGroupsList(response);
    } else {
      this.gwGroups = [];
    }
    this.updateGroupSelectionState();
  }

  private updateGroupSelectionState() {
    if (this.gwGroups.length) {
      if (this.group) {
        this.gwGroups = this.gwGroups.map((g) => ({
          ...g,
          selected: g.content === this.group
        }));
      } else if (!this.groupSelectionCleared) {
        this.onGroupSelection(this.gwGroups[0]);
      } else {
        this.gwGroups = this.gwGroups.map((g) => ({
          ...g,
          selected: false
        }));
      }
      this.gwGroupsEmpty = false;
      this.gwGroupPlaceholder = DEFAULT_PLACEHOLDER;
    } else {
      this.gwGroupsEmpty = true;
      this.gwGroupPlaceholder = $localize`No groups available`;
    }
  }

  private handleGatewayGroupsError(error: any) {
    this.gwGroups = [];
    this.gwGroupsEmpty = true;
    this.gwGroupPlaceholder = $localize`Unable to fetch Gateway groups`;
    if (error?.preventDefault) {
      error?.preventDefault?.();
    }
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
