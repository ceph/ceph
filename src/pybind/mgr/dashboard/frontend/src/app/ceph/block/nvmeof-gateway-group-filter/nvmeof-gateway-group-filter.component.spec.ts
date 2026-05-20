import { ComponentFixture, TestBed } from '@angular/core/testing';

import { NvmeofGatewayGroupFilterComponent } from './nvmeof-gateway-group-filter.component';

describe('NvmeofGatewayGroupFilterComponent', () => {
  let fixture: ComponentFixture<NvmeofGatewayGroupFilterComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [NvmeofGatewayGroupFilterComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(NvmeofGatewayGroupFilterComponent);
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(fixture.componentInstance).toBeTruthy();
  });

  it('should render combo box with filter class', () => {
    const combo = fixture.nativeElement.querySelector('cds-combo-box');
    expect(combo).toBeTruthy();
    expect(combo.classList.contains('nvmeof-gateway-group-filter__combo')).toBe(true);
  });
});
