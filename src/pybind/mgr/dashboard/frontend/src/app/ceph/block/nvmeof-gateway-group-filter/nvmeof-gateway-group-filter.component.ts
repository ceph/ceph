import { Component, EventEmitter, OnDestroy, OnInit, Output } from '@angular/core';
import { ActivatedRoute, Router, RouterModule } from '@angular/router';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';
import { ComboBoxModule, GridModule, LayoutModule } from 'carbon-components-angular';
import { NvmeofService, GroupsComboboxItem } from '~/app/shared/api/nvmeof.service';
import { CephServiceSpec } from '~/app/shared/models/service.interface';

const DEFAULT_PLACEHOLDER = $localize`Enter group name`;

@Component({
  selector: 'cd-nvmeof-gateway-group-filter',
  templateUrl: './nvmeof-gateway-group-filter.component.html',
  styleUrls: ['./nvmeof-gateway-group-filter.component.scss'],
  standalone: true,
  imports: [ComboBoxModule, GridModule, LayoutModule, RouterModule]
})
export class NvmeofGatewayGroupFilterComponent implements OnInit, OnDestroy {
  @Output() groupChange = new EventEmitter<string | null>();

  items: GroupsComboboxItem[] = [];
  disabled = false;
  placeholder = DEFAULT_PLACEHOLDER;

  private destroy$ = new Subject<void>();

  constructor(
    private nvmeofService: NvmeofService,
    private route: ActivatedRoute,
    private router: Router
  ) {}

  ngOnInit(): void {
    this.loadGatewayGroups();
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  onSelected(item: GroupsComboboxItem): void {
    this.syncQueryParam(item.content);
  }

  onClear(): void {
    this.syncQueryParam(null);
  }

  private loadGatewayGroups(): void {
    this.nvmeofService
      .listGatewayGroups()
      .pipe(takeUntil(this.destroy$))
      .subscribe({
        next: (response: CephServiceSpec[][]) => this.onGroupsLoaded(response),
        error: (error: any) => this.onGroupsError(error)
      });
  }

  private onGroupsLoaded(response: CephServiceSpec[][]): void {
    if (response?.[0]?.length) {
      this.items = this.nvmeofService.formatGwGroupsList(response);
    } else {
      this.items = [];
    }
    this.syncSelectionState();
  }

  private onGroupsError(error: any): void {
    this.items = [];
    this.disabled = true;
    this.placeholder = $localize`Unable to fetch Gateway groups`;
    if (error?.preventDefault) {
      error.preventDefault();
    }
    this.groupChange.emit(null);
  }

  private syncSelectionState(): void {
    if (this.items.length) {
      this.disabled = false;
      this.placeholder = DEFAULT_PLACEHOLDER;
      const urlGroup = this.route.snapshot.queryParams['group']?.trim() || null;
      if (!urlGroup) {
        this.syncQueryParam(this.items[0].content);
      } else {
        this.items = this.items.map((g) => ({ ...g, selected: g.content === urlGroup }));
        this.groupChange.emit(urlGroup);
      }
    } else {
      this.disabled = true;
      this.placeholder = $localize`No groups available`;
      this.groupChange.emit(null);
    }
  }

  private syncQueryParam(group: string | null): void {
    const currentGroup = this.route.snapshot.queryParams['group']?.trim() || null;
    if (currentGroup === group) {
      // URL already correct — still emit so the parent gets the current value on init
      this.items = this.items.map((g) => ({ ...g, selected: g.content === group }));
      this.groupChange.emit(group);
      return;
    }

    this.router.navigate([], {
      relativeTo: this.route,
      queryParams: { group: group || null },
      queryParamsHandling: 'merge',
      replaceUrl: true
    });
    this.items = this.items.map((g) => ({ ...g, selected: g.content === group }));
    this.groupChange.emit(group);
  }
}
