import { ComponentFixture, TestBed } from '@angular/core/testing';
import { SmbUsersgroupsResourceSidebarComponent } from './smb-usersgroups-resource-sidebar.component';
import { ActivatedRoute, convertToParamMap, provideRouter, ParamMap } from '@angular/router';
import { BehaviorSubject } from 'rxjs';
import { Component, Input } from '@angular/core';

// Mock the sidebar layout component so Angular doesn't throw errors about unknown elements
@Component({ selector: 'cd-sidebar-layout', template: '', standalone: false })
class MockSidebarLayoutComponent {
  @Input() title: string;
  @Input() items: any[];
}

describe('SmbUsersgroupsResourceSidebarComponent', () => {
  let component: SmbUsersgroupsResourceSidebarComponent;
  let fixture: ComponentFixture<SmbUsersgroupsResourceSidebarComponent>;
  let paramMapSubject: BehaviorSubject<ParamMap>;

  beforeEach(async () => {
    // Use a BehaviorSubject so we can push new route parameters dynamically during tests
    paramMapSubject = new BehaviorSubject<ParamMap>(
      convertToParamMap({ users_groups_id: 'standalone-url-id' })
    );

    const activatedRouteMock = {
      paramMap: paramMapSubject.asObservable()
    };

    await TestBed.configureTestingModule({
      declarations: [SmbUsersgroupsResourceSidebarComponent, MockSidebarLayoutComponent],
      providers: [provideRouter([]), { provide: ActivatedRoute, useValue: activatedRouteMock }]
    }).compileComponents();

    fixture = TestBed.createComponent(SmbUsersgroupsResourceSidebarComponent);
    component = fixture.componentInstance;
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  describe('Route Parameter Initialization (ngOnInit)', () => {
    it('should set usersGroupsIdRoute and build sidebar items when param exists', () => {
      // Act
      fixture.detectChanges(); // Triggers ngOnInit

      // Assert
      expect(component.usersGroupsIdRoute).toBe('standalone-url-id');
      expect(component.sidebarItems.length).toBe(1);

      const overviewItem = component.sidebarItems[0];
      expect(overviewItem.label).toBe('Overview');
      expect(overviewItem.route).toEqual([
        '/cephfs/smb/standalone',
        'standalone-url-id',
        'overview'
      ]);
      expect(overviewItem.routerLinkActiveOptions).toEqual({ exact: true });
    });

    it('should handle missing users_groups_id param gracefully', () => {
      // Arrange
      paramMapSubject.next(convertToParamMap({}));

      // Act
      fixture.detectChanges();

      // Assert
      expect(component.usersGroupsIdRoute).toBe('');
      expect(component.standaloneName).toBe('');

      // Sidebar items should still be built, just with empty route fragments
      expect(component.sidebarItems[0].route).toEqual(['/cephfs/smb/standalone', '', 'overview']);
    });
  });

  describe('Title Loading (loadTitle)', () => {
    it('should use the URL param as the standalone name', () => {
      // Act
      fixture.detectChanges();

      // Assert
      expect(component.standaloneName).toBe('standalone-url-id');
    });

    it('should update standalone name when route param changes', () => {
      // Act
      fixture.detectChanges();
      paramMapSubject.next(convertToParamMap({ users_groups_id: 'another-id' }));

      // Assert
      expect(component.standaloneName).toBe('another-id');
    });
  });

  describe('ngOnDestroy', () => {
    it('should unsubscribe from observables', () => {
      // Arrange
      fixture.detectChanges();
      const unsubscribeSpy = jest.spyOn((component as any).sub, 'unsubscribe');

      // Act
      component.ngOnDestroy();

      // Assert
      expect(unsubscribeSpy).toHaveBeenCalled();
    });
  });
});
