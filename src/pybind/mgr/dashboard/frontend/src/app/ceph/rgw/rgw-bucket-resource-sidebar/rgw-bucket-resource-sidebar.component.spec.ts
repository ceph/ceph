import { ComponentFixture, TestBed } from '@angular/core/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ActivatedRoute, convertToParamMap } from '@angular/router';
import { BehaviorSubject } from 'rxjs';

import { RgwBucketResourceSidebarComponent } from './rgw-bucket-resource-sidebar.component';

describe('RgwBucketResourceSidebarComponent', () => {
  let component: RgwBucketResourceSidebarComponent;
  let fixture: ComponentFixture<RgwBucketResourceSidebarComponent>;
  let paramMapSubject: BehaviorSubject<any>;

  beforeEach(async () => {
    paramMapSubject = new BehaviorSubject(convertToParamMap({ bid: 'test22', owner: 'Account1' }));

    await TestBed.configureTestingModule({
      declarations: [RgwBucketResourceSidebarComponent],
      providers: [
        {
          provide: ActivatedRoute,
          useValue: {
            paramMap: paramMapSubject.asObservable()
          }
        }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();

    fixture = TestBed.createComponent(RgwBucketResourceSidebarComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should initialize bid/owner and build sidebar items from route params', () => {
    expect(component.bid).toBe('test22');
    expect(component.owner).toBe('Account1');

    expect(component.sidebarItems.map((item) => item.label)).toEqual([
      'Configuration',
      'Permissions',
      'Data management',
      'Notifications'
    ]);

    expect(component.sidebarItems[0].route).toEqual([
      '/rgw/bucket',
      'test22',
      'Account1',
      'configuration'
    ]);
  });

  it('should clear the header tag and skip item building when bid is empty', () => {
    paramMapSubject.next(convertToParamMap({ bid: '', owner: 'Account1' }));

    expect(component.bid).toBe('');
    expect(component.sidebarItems).toEqual([]);
  });

  it('should unsubscribe on destroy', () => {
    component.ngOnDestroy();
    expect((component as any).sub.closed).toBe(true);
  });
});
