import { Component, Input, OnDestroy, OnInit } from '@angular/core';
import { NavigationEnd, Router } from '@angular/router';
import { Subscription } from 'rxjs';
import { filter } from 'rxjs/operators';

import { IBreadcrumb } from '~/app/shared/models/breadcrumbs';
import { RouteBreadcrumbsService } from '~/app/shared/services/route-breadcrumbs.service';

export interface ResourceHeaderStatus {
  type: 'success' | 'warning' | 'info' | 'danger';
  text: string;
}

export interface ResourceHeaderAction {
  label: string;
  disabled?: boolean;
  onClick?: () => void;
}

@Component({
  selector: 'cd-page-header-resource',
  templateUrl: './page-header-resource.component.html',
  styleUrls: ['./page-header-resource.component.scss'],
  standalone: false
})
export class PageHeaderResourceComponent implements OnInit, OnDestroy {
  private readonly subscriptions = new Subscription();

  @Input({ required: true }) title: string;
  @Input() status?: ResourceHeaderStatus;
  @Input() tags: string[] = [];
  @Input() actions: ResourceHeaderAction[] = [];
  @Input() showBreadcrumbs = true;

  breadcrumbs: IBreadcrumb[] = [];

  constructor(
    private router: Router,
    private routeBreadcrumbsService: RouteBreadcrumbsService
  ) {}

  ngOnInit(): void {
    this.updateBreadcrumbs();
    this.subscriptions.add(
      this.router.events
        .pipe(filter((event) => event instanceof NavigationEnd))
        .subscribe(() => this.updateBreadcrumbs())
    );
  }

  ngOnDestroy(): void {
    this.subscriptions.unsubscribe();
  }

  runAction(action: ResourceHeaderAction): void {
    if (action.disabled) {
      return;
    }

    action.onClick?.();
  }

  private updateBreadcrumbs(): void {
    if (!this.showBreadcrumbs) {
      this.breadcrumbs = [];
      return;
    }

    this.routeBreadcrumbsService
      .resolve(this.router.routerState.snapshot.root)
      .subscribe((crumbs) => {
        this.breadcrumbs = crumbs;
      });
  }
}
