import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { of } from 'rxjs';

import { NvmeSubsystemViewComponent } from './nvme-subsystem-view.component';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { configureTestBed } from '~/testing/unit-test-helper';
describe('NvmeSubsystemViewComponent', () => {
  let component: NvmeSubsystemViewComponent;
  let fixture: ComponentFixture<NvmeSubsystemViewComponent>;

  const mockParamMap = {
    get: (key: string) => (key === 'subsystem_nqn' ? 'nqn.test' : null)
  };
  const mockQueryParams = { group: 'my-group' };

  const mockActivatedRoute = {
    paramMap: of(mockParamMap),
    queryParams: of(mockQueryParams)
  };

  configureTestBed({
    declarations: [NvmeSubsystemViewComponent],
    imports: [RouterTestingModule, HttpClientTestingModule],
    providers: [{ provide: ActivatedRoute, useFactory: () => mockActivatedRoute }],
    schemas: [CUSTOM_ELEMENTS_SCHEMA]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(NvmeSubsystemViewComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should build sidebar items correctly', () => {
    expect(component.sidebarItems.length).toBe(5);

    expect(component.sidebarItems[0].route).toEqual([
      '/block/nvmeof/subsystems',
      'nqn.test',
      'overview'
    ]);
    expect(component.sidebarItems[0].routeExtras).toEqual({ queryParams: { group: 'my-group' } });

    expect(component.sidebarItems[1].route).toEqual([
      '/block/nvmeof/subsystems',
      'nqn.test',
      'hosts'
    ]);

    expect(component.sidebarItems[2].route).toEqual([
      '/block/nvmeof/subsystems',
      'nqn.test',
      'namespaces'
    ]);

    expect(component.sidebarItems[3].route).toEqual([
      '/block/nvmeof/subsystems',
      'nqn.test',
      'listeners'
    ]);

    expect(component.sidebarItems[4].route).toEqual([
      '/block/nvmeof/subsystems',
      'nqn.test',
      'performance'
    ]);
  });
});
