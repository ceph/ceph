import {
  Component,
  EventEmitter,
  OnDestroy,
  OnInit,
  Output,
  ViewEncapsulation
} from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';
import { ComboBoxModule } from 'carbon-components-angular';
import { NvmeofService, GroupsComboboxItem } from '~/app/shared/api/nvmeof.service';

@Component({
  selector: 'cd-nvmeof-gateway-group-filter',
  templateUrl: './nvmeof-gateway-group-filter.component.html',
  styleUrls: ['./nvmeof-gateway-group-filter.component.scss'],
  standalone: true,
  imports: [ComboBoxModule],
  encapsulation: ViewEncapsulation.None
})
export class NvmeofGatewayGroupFilterComponent implements OnInit, OnDestroy {
  @Output() groupChange = new EventEmitter<string | null>();

  items: GroupsComboboxItem[] = [];
  placeholder: string = '';
  disabled: boolean = false;

  private destroy$ = new Subject<void>();

  constructor(
    private nvmeofService: NvmeofService,
    private router: Router,
    private route: ActivatedRoute
  ) {}

  ngOnInit(): void {
    this.nvmeofService
      .listGatewayGroups()
      .pipe(takeUntil(this.destroy$))
      .subscribe({
        next: (response: any) => {
          if (!response?.[0]?.length) {
            this.disabled = true;
            this.placeholder = $localize`No groups available`;
            this.items = [];
            return;
          }
          const formatted: GroupsComboboxItem[] = this.nvmeofService.formatGwGroupsList(response);
          const currentGroup: string | undefined = this.route.snapshot.queryParams['group'];
          if (currentGroup) {
            this.items = formatted.map((item) => ({
              ...item,
              selected: item.content === currentGroup
            }));
          } else {
            this.items = formatted;
            const first = formatted[0];
            this.router.navigate([], {
              relativeTo: this.route,
              queryParams: { group: first.content },
              queryParamsHandling: 'merge',
              replaceUrl: true
            });
          }
        },
        error: () => {
          this.disabled = true;
          this.placeholder = $localize`Unable to fetch Gateway groups`;
          this.items = [];
        }
      });
  }

  onSelected(item: GroupsComboboxItem): void {
    this.groupChange.emit(item.content);
  }

  onClear(): void {
    this.groupChange.emit(null);
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
