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

  it('should emit selected event with the item when onSelected is called', () => {
    const emitSpy = jest.spyOn(fixture.componentInstance.selected, 'emit');
    const item = { content: 'grp1', id: 'grp1' };
    fixture.componentInstance.onSelected(item);
    expect(emitSpy).toHaveBeenCalledWith(item);
  });

  it('should emit cleared event when onClear is called', () => {
    const emitSpy = jest.spyOn(fixture.componentInstance.cleared, 'emit');
    fixture.componentInstance.onClear();
    expect(emitSpy).toHaveBeenCalled();
  });
});
