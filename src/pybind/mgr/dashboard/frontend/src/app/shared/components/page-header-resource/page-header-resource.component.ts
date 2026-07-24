import { Component, Input, OnDestroy, OnInit, ViewEncapsulation } from '@angular/core';
import { NavigationEnd, Router } from '@angular/router';
import { Subscription } from 'rxjs';
import { filter } from 'rxjs/operators';

export interface ResourceHeaderStatus {
  type: 'success' | 'warning' | 'info';
  text: string;
}

export interface ResourceHeaderAction {
  label: string;
  disabled?: boolean;
  onClick?: () => void;
}

interface ResourceHeaderBreadcrumb {
  text: string;
  route?: string[];
}

@Component({
  selector: 'cd-page-header-resource',
  templateUrl: './page-header-resource.component.html',
  styleUrls: ['./page-header-resource.component.scss'],
  encapsulation: ViewEncapsulation.None,
  standalone: false
})
export class PageHeaderResourceComponent implements OnInit, OnDestroy {
  private readonly subscriptions = new Subscription();

  @Input() title?: string;
  @Input() status?: ResourceHeaderStatus;
  @Input() tags: string[] = [];
  @Input() actions: ResourceHeaderAction[] = [];
  @Input() showBreadcrumbs = true;

  breadcrumbs: ResourceHeaderBreadcrumb[] = [];

  constructor(private router: Router) {}

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

  get statusIconType(): string {
    if (this.status?.type === 'success') {
      return 'success';
    }

    if (this.status?.type === 'warning') {
      return 'warning';
    }

    return 'infoCircle';
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

    const path = this.router.url.split('?')[0];
    const segments = path.split('/').filter(Boolean);
    const crumbs: ResourceHeaderBreadcrumb[] = [];

    if (segments[0] === 'hosts') {
      crumbs.push({ text: 'Cluster' });
    }

    const routeAcc: string[] = [];
    segments.forEach((segment, index) => {
      routeAcc.push(segment);
      crumbs.push({
        text: this.formatSegment(segment),
        route: index < segments.length - 1 ? ['/', ...routeAcc] : undefined
      });
    });

    this.breadcrumbs = crumbs;
  }

  private formatSegment(segment: string): string {
    if (segment.includes('-') && /\d/.test(segment)) {
      return segment;
    }

    return segment
      .split('-')
      .map((part) => (part ? part[0].toUpperCase() + part.slice(1) : part))
      .join(' ');
  }
}
