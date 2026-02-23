import { ComponentFixture, TestBed, fakeAsync, tick } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { ActivatedRoute } from '@angular/router';
import { of } from 'rxjs';

import { NgbTooltipModule } from '@ng-bootstrap/ng-bootstrap';
import { GridModule, TilesModule } from 'carbon-components-angular';

import { NvmeofSubsystemOverviewComponent } from './nvmeof-subsystem-overview.component';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { SharedModule } from '~/app/shared/shared.module';

describe('NvmeofSubsystemOverviewComponent', () => {
  let component: NvmeofSubsystemOverviewComponent;
  let fixture: ComponentFixture<NvmeofSubsystemOverviewComponent>;
  let nvmeofService: NvmeofService;

  const mockSubsystem = {
    nqn: 'nqn.2016-06.io.spdk:cnode1',
    serial_number: 'Ceph30487186726692',
    model_number: 'Ceph bdev Controller',
    min_cntlid: 1,
    max_cntlid: 2040,
    subtype: 'NVMe',
    namespace_count: 3,
    max_namespaces: 256,
    enable_ha: true,
    allow_any_host: true,
    gw_group: 'gateway-prod',
    psk: 'some-key'
  };

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [NvmeofSubsystemOverviewComponent],
      imports: [
        HttpClientTestingModule,
        RouterTestingModule,
        SharedModule,
        NgbTooltipModule,
        TilesModule,
        GridModule
      ],
      providers: [
        {
          provide: ActivatedRoute,
          useValue: {
            parent: {
              params: of({ subsystem_nqn: 'nqn.2016-06.io.spdk:cnode1' })
            },
            queryParams: of({ group: 'group1' })
          }
        },
        {
          provide: NvmeofService,
          useValue: {
            getSubsystem: jest.fn().mockReturnValue(of(mockSubsystem))
          }
        }
      ]
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(NvmeofSubsystemOverviewComponent);
    component = fixture.componentInstance;
    nvmeofService = TestBed.inject(NvmeofService);
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should fetch subsystem on init', fakeAsync(() => {
    component.ngOnInit();
    tick();
    expect(nvmeofService.getSubsystem).toHaveBeenCalledWith('nqn.2016-06.io.spdk:cnode1', 'group1');
  }));

  it('should store subsystem data', fakeAsync(() => {
    component.ngOnInit();
    tick();
    expect(component.subsystem).toEqual(mockSubsystem);
    expect(component.subsystem.serial_number).toBe('Ceph30487186726692');
    expect(component.subsystem.model_number).toBe('Ceph bdev Controller');
    expect(component.subsystem.max_cntlid).toBe(2040);
    expect(component.subsystem.min_cntlid).toBe(1);
    expect(component.subsystem.namespace_count).toBe(3);
    expect(component.subsystem.max_namespaces).toBe(256);
    expect(component.subsystem.gw_group).toBe('gateway-prod');
  }));

  it('should not fetch when subsystemNQN is missing', fakeAsync(() => {
    TestBed.resetTestingModule();
    TestBed.configureTestingModule({
      declarations: [NvmeofSubsystemOverviewComponent],
      imports: [
        HttpClientTestingModule,
        RouterTestingModule,
        SharedModule,
        NgbTooltipModule,
        TilesModule,
        GridModule
      ],
      providers: [
        {
          provide: ActivatedRoute,
          useValue: {
            parent: {
              params: of({})
            },
            queryParams: of({ group: 'group1' })
          }
        },
        {
          provide: NvmeofService,
          useValue: {
            getSubsystem: jest.fn().mockReturnValue(of(mockSubsystem))
          }
        }
      ]
    }).compileComponents();

    const newFixture = TestBed.createComponent(NvmeofSubsystemOverviewComponent);
    const newComponent = newFixture.componentInstance;
    const newService = TestBed.inject(NvmeofService);
    newFixture.detectChanges();
    tick();
    expect(newService.getSubsystem).not.toHaveBeenCalled();
    expect(newComponent.subsystem).toBeUndefined();
  }));

  it('should render detail labels in the template', fakeAsync(() => {
    component.ngOnInit();
    tick();
    fixture.detectChanges();

    const compiled = fixture.nativeElement;
    const labels = compiled.querySelectorAll('.cds--type-label-01');
    const labelTexts = Array.from(labels).map((el: HTMLElement) => el.textContent.trim());
    expect(labelTexts).toContain('Serial number');
    expect(labelTexts).toContain('Model Number');
    expect(labelTexts).toContain('Gateway group');
    expect(labelTexts).toContain('Maximum Controller Identifier');
    expect(labelTexts).toContain('Minimum Controller Identifier');
    expect(labelTexts).toContain('Namespaces');
    expect(labelTexts).toContain('Maximum allowed namespaces');
  }));

  it('should display subsystem type from subsystem data', fakeAsync(() => {
    component.ngOnInit();
    tick();
    fixture.detectChanges();

    const values = fixture.nativeElement.querySelectorAll('.cds--type-body-compact-01');
    const valueTexts = Array.from(values).map((el: HTMLElement) => el.textContent.trim());
    expect(valueTexts).toContain('NVMe');
  }));

  it('should display hosts allowed from subsystem data', fakeAsync(() => {
    component.ngOnInit();
    tick();
    fixture.detectChanges();

    const values = fixture.nativeElement.querySelectorAll('.cds--type-body-compact-01');
    const valueTexts = Array.from(values).map((el: HTMLElement) => el.textContent.trim());
    expect(valueTexts).toContain('Any host');
  }));

  it('should display HA status from subsystem data', fakeAsync(() => {
    component.ngOnInit();
    tick();
    fixture.detectChanges();

    const values = fixture.nativeElement.querySelectorAll('.cds--type-body-compact-01');
    const valueTexts = Array.from(values).map((el: HTMLElement) => el.textContent.trim());
    expect(valueTexts).toContain('Yes');
  }));
});
