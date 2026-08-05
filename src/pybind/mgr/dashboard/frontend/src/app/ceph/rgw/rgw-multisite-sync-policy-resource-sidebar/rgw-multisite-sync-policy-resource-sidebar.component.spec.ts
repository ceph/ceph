import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ActivatedRoute, ParamMap, convertToParamMap } from '@angular/router';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { BehaviorSubject } from 'rxjs';

import { RgwMultisiteSyncPolicyResourceSidebarComponent } from './rgw-multisite-sync-policy-resource-sidebar.component';

describe('RgwMultisiteSyncPolicyResourceSidebarComponent', () => {
  let component: RgwMultisiteSyncPolicyResourceSidebarComponent;
  let fixture: ComponentFixture<RgwMultisiteSyncPolicyResourceSidebarComponent>;

  let paramMapSubject: BehaviorSubject<ParamMap>;
  let queryParamMapSubject: BehaviorSubject<ParamMap>;

  beforeEach(async () => {
    // Use convertToParamMap to generate a fully compliant ParamMap object
    paramMapSubject = new BehaviorSubject<ParamMap>(
      convertToParamMap({ groupName: 'sync-group-a' })
    );
    queryParamMapSubject = new BehaviorSubject<ParamMap>(convertToParamMap({}));

    await TestBed.configureTestingModule({
      declarations: [RgwMultisiteSyncPolicyResourceSidebarComponent],
      providers: [
        {
          provide: ActivatedRoute,
          useValue: {
            paramMap: paramMapSubject.asObservable(),
            queryParamMap: queryParamMapSubject.asObservable()
          }
        }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();

    fixture = TestBed.createComponent(RgwMultisiteSyncPolicyResourceSidebarComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should build sidebar items correctly without a bucketName (group scope)', () => {
    expect(component.groupName).toBe('sync-group-a');
    expect(component.sidebarItems.length).toBe(4);

    const overviewItem = component.sidebarItems[0];
    expect(overviewItem.label).toBe('Overview');
    expect(overviewItem.route).toEqual(['/rgw/multisite/sync-policy', 'sync-group-a', 'overview']);
    // Should have empty routeExtras since bucketName is null
    expect(overviewItem.routeExtras).toEqual({});
  });

  it('should build sidebar items correctly with a bucketName (bucket scope)', () => {
    // Simulate navigating to a bucket-scoped policy
    paramMapSubject.next(convertToParamMap({ groupName: 'sync-group-b' }));
    queryParamMapSubject.next(convertToParamMap({ bucketName: 'my-test-bucket' }));

    // Trigger the subscription update
    fixture.detectChanges();

    expect(component.groupName).toBe('sync-group-b');
    expect(component.sidebarItems.length).toBe(4);

    const pipeItem = component.sidebarItems[3];
    expect(pipeItem.label).toBe('Pipe');
    expect(pipeItem.route).toEqual(['/rgw/multisite/sync-policy', 'sync-group-b', 'pipe']);
    // routeExtras should now contain the queryParams
    expect(pipeItem.routeExtras).toEqual({ queryParams: { bucketName: 'my-test-bucket' } });
  });

  it('should handle missing groupName safely', () => {
    // Simulate missing groupName parameter entirely
    paramMapSubject.next(convertToParamMap({}));
    fixture.detectChanges();

    expect(component.groupName).toBe('');

    const overviewItem = component.sidebarItems[0];
    // Route should fall back to an empty string where the groupName normally goes
    expect(overviewItem.route).toEqual(['/rgw/multisite/sync-policy', '', 'overview']);
  });

  it('should unsubscribe from observables on destroy', () => {
    const unsubscribeSpy = jest.spyOn((component as any).sub, 'unsubscribe');

    component.ngOnDestroy();

    expect(unsubscribeSpy).toHaveBeenCalled();
  });
});
