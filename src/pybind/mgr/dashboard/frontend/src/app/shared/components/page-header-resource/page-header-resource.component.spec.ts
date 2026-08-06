import { ComponentFixture, TestBed } from '@angular/core/testing';
import { NavigationEnd, Router } from '@angular/router';
import { NO_ERRORS_SCHEMA, Pipe, PipeTransform } from '@angular/core';
import { of, Subject } from 'rxjs';

import {
  PageHeaderResourceComponent,
  ResourceHeaderAction
} from './page-header-resource.component';
import { RouteBreadcrumbsService } from '~/app/shared/services/route-breadcrumbs.service';
import { IBreadcrumb } from '~/app/shared/models/breadcrumbs';

// 1. Mock the overviewStatus pipe because the template evaluates .icon on its result
@Pipe({
  name: 'overviewStatus',
  standalone: false
})
class MockOverviewStatusPipe implements PipeTransform {
  transform(value: string) {
    return { icon: `mock-${value}-icon` };
  }
}

describe('PageHeaderResourceComponent', () => {
  let component: PageHeaderResourceComponent;
  let fixture: ComponentFixture<PageHeaderResourceComponent>;

  let mockRouter: any;
  let mockRouteBreadcrumbsService: any;
  let routerEventsSubject: Subject<any>;

  const mockBreadcrumbs: IBreadcrumb[] = [
    { text: 'Home', path: 'home' },
    { text: 'Resource', path: 'resource' }
  ];

  beforeEach(async () => {
    // Setup a Subject to control router events
    routerEventsSubject = new Subject<any>();

    // Mock the Router
    mockRouter = {
      events: routerEventsSubject.asObservable(),
      routerState: {
        snapshot: {
          root: 'mock-root'
        }
      }
    };

    // Mock the RouteBreadcrumbsService
    mockRouteBreadcrumbsService = {
      resolve: jest.fn().mockReturnValue(of(mockBreadcrumbs))
    };

    await TestBed.configureTestingModule({
      declarations: [PageHeaderResourceComponent, MockOverviewStatusPipe],
      providers: [
        { provide: Router, useValue: mockRouter },
        { provide: RouteBreadcrumbsService, useValue: mockRouteBreadcrumbsService }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();

    fixture = TestBed.createComponent(PageHeaderResourceComponent);
    component = fixture.componentInstance;

    // Set the required input
    component.title = 'Test Resource';
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  describe('Breadcrumbs', () => {
    it('should fetch breadcrumbs on init if showBreadcrumbs is true', () => {
      component.showBreadcrumbs = true;
      fixture.detectChanges(); // triggers ngOnInit

      expect(mockRouteBreadcrumbsService.resolve).toHaveBeenCalledWith('mock-root');
      expect(component.breadcrumbs).toEqual(mockBreadcrumbs);
    });

    it('should NOT fetch breadcrumbs on init if showBreadcrumbs is false', () => {
      component.showBreadcrumbs = false;
      fixture.detectChanges(); // triggers ngOnInit

      expect(mockRouteBreadcrumbsService.resolve).not.toHaveBeenCalled();
      expect(component.breadcrumbs).toEqual([]);
    });

    it('should update breadcrumbs when NavigationEnd event is emitted', () => {
      component.showBreadcrumbs = true;
      fixture.detectChanges();

      // Clear the initial call made during ngOnInit
      mockRouteBreadcrumbsService.resolve.mockClear();

      // Emit a NavigationEnd event
      routerEventsSubject.next(new NavigationEnd(1, '/test', '/test'));

      expect(mockRouteBreadcrumbsService.resolve).toHaveBeenCalledWith('mock-root');
    });

    it('should ignore other router events', () => {
      component.showBreadcrumbs = true;
      fixture.detectChanges();

      mockRouteBreadcrumbsService.resolve.mockClear();

      // Emit a non-NavigationEnd event (e.g., just a plain object or different event type)
      routerEventsSubject.next({ type: 'NavigationStart' });

      expect(mockRouteBreadcrumbsService.resolve).not.toHaveBeenCalled();
    });
  });

  describe('Actions', () => {
    it('should execute action onClick if not disabled', () => {
      const mockAction: ResourceHeaderAction = {
        label: 'Test Action',
        disabled: false,
        onClick: jest.fn()
      };

      component.runAction(mockAction);

      expect(mockAction.onClick).toHaveBeenCalled();
    });

    it('should NOT execute action onClick if disabled', () => {
      const mockAction: ResourceHeaderAction = {
        label: 'Test Action',
        disabled: true,
        onClick: jest.fn()
      };

      component.runAction(mockAction);

      expect(mockAction.onClick).not.toHaveBeenCalled();
    });

    it('should safely do nothing if onClick is undefined', () => {
      const mockAction: ResourceHeaderAction = {
        label: 'Test Action'
      };

      expect(() => component.runAction(mockAction)).not.toThrow();
    });
  });
});
