import { CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed, waitForAsync } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { SideNavModule, ThemeModule } from 'carbon-components-angular';

import { NvmeSubsystemViewComponent } from './nvme-subsystem-view.component';
import { HttpClientTestingModule } from '@angular/common/http/testing';

describe('NvmeSubsystemViewComponent', () => {
  let component: NvmeSubsystemViewComponent;
  let fixture: ComponentFixture<NvmeSubsystemViewComponent>;

  beforeEach(
    waitForAsync(() => {
      TestBed.configureTestingModule({
        declarations: [NvmeSubsystemViewComponent],
        imports: [RouterTestingModule, SideNavModule, ThemeModule, HttpClientTestingModule],
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
});
