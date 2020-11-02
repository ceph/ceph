import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { IdentityComponent } from './identity.component';

describe('IdentityComponent', () => {
  let component: IdentityComponent;
  let fixture: ComponentFixture<IdentityComponent>;

  configureTestBed({
    imports: [HttpClientTestingModule, SharedModule, RouterTestingModule],
    declarations: [IdentityComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(IdentityComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
