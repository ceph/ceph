import { CommonModule } from '@angular/common';
import { Component } from '@angular/core';
import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { Router, Routes } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { PerformanceCounterBreadcrumbsResolver } from '../../../app-routing.module';
import { BreadcrumbsComponent } from './breadcrumbs.component';

describe('BreadcrumbsComponent', () => {
  let component: BreadcrumbsComponent;
  let fixture: ComponentFixture<BreadcrumbsComponent>;
  let router: Router;

  @Component({ selector: 'cd-fake', template: '' })
  class FakeComponent {}

  const routes: Routes = [
    {
      path: 'hosts',
      component: FakeComponent,
      data: { breadcrumbs: 'Cluster/Hosts' }
    },
    {
      path: 'perf_counters',
      component: FakeComponent,
      data: {
        breadcrumbs: PerformanceCounterBreadcrumbsResolver
      }
    },
    {
      path: 'block',
      data: { breadcrumbs: true, text: 'Block', path: null },
      children: [
        {
          path: 'rbd',
          data: { breadcrumbs: 'Images' },
          children: [
            { path: '', component: FakeComponent },
            { path: 'add', component: FakeComponent, data: { breadcrumbs: 'Add' } }
          ]
        }
      ]
    }
  ];

  configureTestBed({
    declarations: [BreadcrumbsComponent, FakeComponent],
    imports: [CommonModule, RouterTestingModule.withRoutes(routes)],
    providers: [PerformanceCounterBreadcrumbsResolver]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(BreadcrumbsComponent);
    router = TestBed.inject(Router);
    component = fixture.componentInstance;
    fixture.detectChanges();
    expect(component.crumbs).toEqual([]);
  });

  it('should create', () => {
    expect(component).toBeTruthy();
    expect(component.subscription).toBeDefined();
  });

  it('should run postProcess and split the breadcrumbs when navigating to hosts', fakeAsync(() => {
    fixture.ngZone.run(() => {
      router.navigateByUrl('/hosts');
    });
    tick();
    expect(component.crumbs).toEqual([
      { path: null, text: 'Cluster' },
      { path: '/hosts', text: 'Hosts' }
    ]);
  }));

  it('should display empty breadcrumb when navigating to perf_counters from unknown path', fakeAsync(() => {
    fixture.ngZone.run(() => {
      router.navigateByUrl('/perf_counters');
    });
    tick();
    expect(component.crumbs).toEqual([
      { path: null, text: 'Cluster' },
      { path: null, text: '' },
      { path: '', text: 'Performance Counters' }
    ]);
  }));

  it('should display Monitor breadcrumb when navigating to perf_counters from Monitors', fakeAsync(() => {
    fixture.ngZone.run(() => {
      router.navigate(['/perf_counters'], { queryParams: { fromLink: '/monitor' } });
    });
    tick();
    expect(component.crumbs).toEqual([
      { path: null, text: 'Cluster' },
      { path: '/monitor', text: 'Monitors' },
      { path: '', text: 'Performance Counters' }
    ]);
  }));

  it('should display Hosts breadcrumb when navigating to perf_counters from Hosts', fakeAsync(() => {
    fixture.ngZone.run(() => {
      router.navigate(['/perf_counters'], { queryParams: { fromLink: '/hosts' } });
    });
    tick();
    expect(component.crumbs).toEqual([
      { path: null, text: 'Cluster' },
      { path: '/hosts', text: 'Hosts' },
      { path: '', text: 'Performance Counters' }
    ]);
  }));

  it('should show all 3 breadcrumbs when navigating to RBD Add', fakeAsync(() => {
    fixture.ngZone.run(() => {
      router.navigateByUrl('/block/rbd/add');
    });
    tick();
    expect(component.crumbs).toEqual([
      { path: null, text: 'Block' },
      { path: '/block/rbd', text: 'Images' },
      { path: '/block/rbd/add', text: 'Add' }
    ]);
  }));

  it('should unsubscribe on ngOnDestroy', () => {
    expect(component.subscription.closed).toBeFalsy();
    component.ngOnDestroy();
    expect(component.subscription.closed).toBeTruthy();
  });
});
