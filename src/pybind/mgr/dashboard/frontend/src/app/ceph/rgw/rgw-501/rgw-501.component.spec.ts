import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { SharedModule } from '../../../shared/shared.module';
import { Rgw501Component } from './rgw-501.component';

describe('Rgw501Component', () => {
  let component: Rgw501Component;
  let fixture: ComponentFixture<Rgw501Component>;

  configureTestBed({
    declarations: [Rgw501Component],
    imports: [HttpClientTestingModule, RouterTestingModule, SharedModule]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(Rgw501Component);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
