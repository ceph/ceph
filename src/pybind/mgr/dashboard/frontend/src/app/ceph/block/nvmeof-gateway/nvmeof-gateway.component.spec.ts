import { ComponentFixture, TestBed } from '@angular/core/testing';

import { NvmeofGatewayComponent } from './nvmeof-gateway.component';

import { HttpClientModule } from '@angular/common/http';
import { SharedModule } from '~/app/shared/shared.module';
import { ComboBoxModule, GridModule, TabsModule } from 'carbon-components-angular';

describe('NvmeofGatewayComponent', () => {
  let component: NvmeofGatewayComponent;
  let fixture: ComponentFixture<NvmeofGatewayComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [NvmeofGatewayComponent],
      imports: [HttpClientModule, SharedModule, ComboBoxModule, GridModule, TabsModule],
      providers: []
    }).compileComponents();

    fixture = TestBed.createComponent(NvmeofGatewayComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
