import { CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed, waitForAsync } from '@angular/core/testing';
import { SideNavModule, ThemeModule } from 'carbon-components-angular';

import { RouterTestingModule } from '@angular/router/testing';
import { NvmeGatewayViewComponent } from './nvme-gateway-view.component';

describe('NvmeGatewayViewComponent', () => {
  let component: NvmeGatewayViewComponent;
  let fixture: ComponentFixture<NvmeGatewayViewComponent>;

  beforeEach(
    waitForAsync(() => {
      TestBed.configureTestingModule({
        declarations: [NvmeGatewayViewComponent],
        imports: [RouterTestingModule, SideNavModule, ThemeModule],
        schemas: [CUSTOM_ELEMENTS_SCHEMA]
      }).compileComponents();
    })
  );

  beforeEach(() => {
    fixture = TestBed.createComponent(NvmeGatewayViewComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
