import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { CephLogo } from './ceph-logo.component';

describe('CephLogoComponent', () => {
  let component: CephLogo;
  let fixture: ComponentFixture<CephLogo>;

  configureTestBed({
    imports: [RouterTestingModule],
    declarations: [CephLogo]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CephLogo);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
