import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RgwStorageClassResourceSidebarComponent } from './rgw-storage-class-resource-sidebar.component';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { SharedModule } from '~/app/shared/shared.module';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { ActivatedRoute, convertToParamMap } from '@angular/router';
import { BehaviorSubject } from 'rxjs';

describe('RgwStorageClassResourceSidebarComponent', () => {
  let component: RgwStorageClassResourceSidebarComponent;
  let fixture: ComponentFixture<RgwStorageClassResourceSidebarComponent>;
  let paramMapSubject: BehaviorSubject<any>;

  beforeEach(async () => {
    // Create a BehaviorSubject to simulate route parameter changes
    paramMapSubject = new BehaviorSubject(convertToParamMap({}));

    await TestBed.configureTestingModule({
      imports: [
        BrowserAnimationsModule,
        SharedModule,
        HttpClientTestingModule,
        RouterTestingModule
      ],
      declarations: [RgwStorageClassResourceSidebarComponent],
      providers: [
        {
          provide: ActivatedRoute,
          useValue: {
            paramMap: paramMapSubject.asObservable()
          }
        }
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(RgwStorageClassResourceSidebarComponent);
    component = fixture.componentInstance;

    // Initial change detection
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('Route Parameter Handling', () => {
    it('should set isResourcePage to true and build sidebar items when all params are present', () => {
      // Act: Push new mock route parameters
      paramMapSubject.next(
        convertToParamMap({
          zonegroup_name: 'default',
          placement_target: 'default-placement',
          storage_class: 'test-sc'
        })
      );

      // Assert: Verify component state updated correctly
      expect(component.zonegroupName).toBe('default');
      expect(component.placementTarget).toBe('default-placement');
      expect(component.storageClassTitle).toBe('test-sc');
      expect(component.isResourcePage).toBe(true);
      expect(component.sidebarItems.length).toBe(2);
    });

    it('should set isResourcePage to false and NOT build items if zonegroup_name is missing', () => {
      // Act
      paramMapSubject.next(
        convertToParamMap({
          placement_target: 'default-placement',
          storage_class: 'test-sc'
        })
      );

      // Assert
      expect(component.isResourcePage).toBe(false);
      expect(component.sidebarItems.length).toBe(0);
    });

    it('should set isResourcePage to false and NOT build items if placement_target is missing', () => {
      // Act
      paramMapSubject.next(
        convertToParamMap({
          zonegroup_name: 'default',
          storage_class: 'test-sc'
        })
      );

      // Assert
      expect(component.isResourcePage).toBe(false);
      expect(component.sidebarItems.length).toBe(0);
    });

    it('should set isResourcePage to false and NOT build items if storage_class is missing', () => {
      // Act
      paramMapSubject.next(
        convertToParamMap({
          zonegroup_name: 'default',
          placement_target: 'default-placement'
        })
      );

      // Assert
      expect(component.isResourcePage).toBe(false);
      expect(component.sidebarItems.length).toBe(0);
    });
  });

  describe('Sidebar Item Construction', () => {
    it('should construct correct routes for Overview and Policy items', () => {
      // Act
      paramMapSubject.next(
        convertToParamMap({
          zonegroup_name: 'zg1',
          placement_target: 'pt1',
          storage_class: 'sc1'
        })
      );

      // Assert
      const overviewItem = component.sidebarItems[0];
      const policyItem = component.sidebarItems[1];

      expect(overviewItem.label).toBe('Overview');
      expect(overviewItem.route).toEqual(['/rgw/storage-class', 'zg1', 'pt1', 'sc1', 'overview']);
      expect(overviewItem.routerLinkActiveOptions).toEqual({ exact: true });

      expect(policyItem.label).toBe('Policy');
      expect(policyItem.route).toEqual(['/rgw/storage-class', 'zg1', 'pt1', 'sc1', 'policy']);
      expect(policyItem.routerLinkActiveOptions).toEqual({ exact: true });
    });
  });

  describe('Lifecycle Hooks', () => {
    it('should unsubscribe from observables on destroy', () => {
      // Arrange: Spy on the Subscription's unsubscribe method
      const unsubscribeSpy = spyOn((component as any).sub, 'unsubscribe');

      // Act
      component.ngOnDestroy();

      // Assert
      expect(unsubscribeSpy).toHaveBeenCalled();
    });
  });
});
