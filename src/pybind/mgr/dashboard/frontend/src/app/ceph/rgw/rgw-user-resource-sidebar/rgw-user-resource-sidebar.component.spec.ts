import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ActivatedRoute } from '@angular/router';
import { of } from 'rxjs';

import { configureTestBed } from '~/testing/unit-test-helper';
import { RgwUserResourceSidebarComponent } from './rgw-user-resource-sidebar.component';
import { RgwUser } from '../models/rgw-user';

describe('RgwUserResourceSidebarComponent', () => {
  let component: RgwUserResourceSidebarComponent;
  let fixture: ComponentFixture<RgwUserResourceSidebarComponent>;

  const mockActivatedRoute = {
    paramMap: of({ get: (key: string) => (key === 'uid' ? 'test-user-id' : null) }),
    data: of({ user: { uid: 'test-user-id' } as RgwUser })
  };

  configureTestBed({
    declarations: [RgwUserResourceSidebarComponent],
    providers: [{ provide: ActivatedRoute, useValue: mockActivatedRoute }]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwUserResourceSidebarComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should set uid and build sidebar items on init', () => {
    expect(component.uid).toBe('test-user-id');
    expect(component.sidebarItems.length).toBe(1);
    expect(component.sidebarItems[0].label).toBe('Overview');
    expect(component.sidebarItems[0].route).toEqual(['/rgw/user', 'test-user-id', 'overview']);
  });

  it('should set user from route data on init', () => {
    expect(component.user).toBeDefined();
    expect(component.user?.uid).toBe('test-user-id');
  });
});
