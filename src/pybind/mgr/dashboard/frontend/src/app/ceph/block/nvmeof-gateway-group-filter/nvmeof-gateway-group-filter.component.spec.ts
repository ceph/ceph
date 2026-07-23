import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ActivatedRoute, Router } from '@angular/router';
import { of, throwError } from 'rxjs';

import { NvmeofGatewayGroupFilterComponent } from './nvmeof-gateway-group-filter.component';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';

const MOCK_GROUPS_RESPONSE = [
  [
    { spec: { group: 'grp1' }, service_name: 'nvmeof.grp1' },
    { spec: { group: 'grp2' }, service_name: 'nvmeof.grp2' }
  ]
];

describe('NvmeofGatewayGroupFilterComponent', () => {
  let fixture: ComponentFixture<NvmeofGatewayGroupFilterComponent>;
  let component: NvmeofGatewayGroupFilterComponent;
  let nvmeofService: Partial<NvmeofService>;
  let routerSpy: Partial<Router>;
  let activatedRoute: Partial<ActivatedRoute>;

  beforeEach(async () => {
    nvmeofService = {
      listGatewayGroups: jest.fn().mockReturnValue(of(MOCK_GROUPS_RESPONSE)),
      formatGwGroupsList: jest.fn().mockReturnValue([{ content: 'grp1' }, { content: 'grp2' }])
    };

    routerSpy = {
      navigate: jest.fn().mockResolvedValue(true)
    };

    activatedRoute = {
      snapshot: {
        queryParams: {}
      } as any
    };

    await TestBed.configureTestingModule({
      imports: [NvmeofGatewayGroupFilterComponent],
      providers: [
        { provide: NvmeofService, useValue: nvmeofService },
        { provide: Router, useValue: routerSpy },
        { provide: ActivatedRoute, useValue: activatedRoute }
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(NvmeofGatewayGroupFilterComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  it('should load gateway groups on init', () => {
    fixture.detectChanges();
    expect(nvmeofService.listGatewayGroups).toHaveBeenCalled();
    expect(component.items.length).toBe(2);
  });

  it('should default to the first group when no group is in the URL', () => {
    fixture.detectChanges();
    expect(routerSpy.navigate).toHaveBeenCalledWith(
      [],
      expect.objectContaining({ queryParams: { group: 'grp1' } })
    );
  });

  it('should mark the URL group as selected when a group is already in the URL', () => {
    (activatedRoute as any).snapshot.queryParams = { group: 'grp2' };
    fixture.detectChanges();
    const selected = component.items.find((i) => i.selected);
    expect(selected?.content).toBe('grp2');
  });

  it('should emit groupChange when a group is selected', () => {
    fixture.detectChanges();
    const emitSpy = jest.spyOn(component.groupChange, 'emit');
    component.onSelected({ content: 'grp2' });
    expect(emitSpy).toHaveBeenCalledWith('grp2');
  });

  it('should emit null groupChange when cleared', () => {
    fixture.detectChanges();
    const emitSpy = jest.spyOn(component.groupChange, 'emit');
    component.onClear();
    expect(emitSpy).toHaveBeenCalledWith(null);
  });

  it('should disable and set placeholder when no groups are available', () => {
    (nvmeofService.listGatewayGroups as jest.Mock).mockReturnValue(of([[]]));
    fixture.detectChanges();
    expect(component.disabled).toBe(true);
    expect(component.placeholder).toBe('No groups available');
  });

  it('should disable and set error placeholder on API error', () => {
    const err = new Error('network error');
    (nvmeofService.listGatewayGroups as jest.Mock).mockReturnValue(throwError(() => err));
    fixture.detectChanges();
    expect(component.disabled).toBe(true);
    expect(component.placeholder).toBe('Unable to fetch Gateway groups');
  });

  it('should handle non-Error API failures by disabling the control', () => {
    const err = { message: 'network error' };
    (nvmeofService.listGatewayGroups as jest.Mock).mockReturnValue(throwError(() => err));
    fixture.detectChanges();
    expect(component.disabled).toBe(true);
    expect(component.placeholder).toBe('Unable to fetch Gateway groups');
    expect(component.items).toEqual([]);
  });

  it('should render a cds-combo-box', () => {
    fixture.detectChanges();
    const combo = fixture.nativeElement.querySelector('cds-combo-box');
    expect(combo).toBeTruthy();
  });
});
