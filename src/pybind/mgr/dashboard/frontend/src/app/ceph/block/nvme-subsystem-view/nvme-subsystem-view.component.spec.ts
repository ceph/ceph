import { ComponentFixture, TestBed, waitForAsync } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { of } from 'rxjs';

import { NvmeSubsystemViewComponent } from './nvme-subsystem-view.component';
import { HttpClientTestingModule } from '@angular/common/http/testing';

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

  beforeEach(
    waitForAsync(() => {
      TestBed.configureTestingModule({
        declarations: [NvmeSubsystemViewComponent],
        imports: [RouterTestingModule, HttpClientTestingModule],
        providers: [{ provide: ActivatedRoute, useValue: mockActivatedRoute }],
        schemas: [CUSTOM_ELEMENTS_SCHEMA]
      }).compileComponents();
    })
  );

  beforeEach(() => {
    fixture = TestBed.createComponent(NvmeSubsystemViewComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should build sidebar items correctly', () => {
    expect(component.sidebarItems.length).toBe(3);

    // Verify first item (Initiators)
    expect(component.sidebarItems[0].route).toEqual([
      '/block/nvmeof/subsystems',
      'nqn.test',
      'hosts'
    ]);
    expect(component.sidebarItems[0].routeExtras).toEqual({ queryParams: { group: 'my-group' } });

    // Verify second item (Namespaces)
    expect(component.sidebarItems[1].route).toEqual([
      '/block/nvmeof/subsystems',
      'nqn.test',
      'namespaces'
    ]);

    // Verify third item (Listeners)
    expect(component.sidebarItems[2].route).toEqual([
      '/block/nvmeof/subsystems',
      'nqn.test',
      'listeners'
    ]);
  });
});
