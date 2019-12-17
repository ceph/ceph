import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { NgBootstrapFormValidationModule } from 'ng-bootstrap-form-validation';
import { BsModalRef } from 'ngx-bootstrap/modal';
import { ToastrModule } from 'ngx-toastr';

import {
  configureTestBed,
  FixtureHelper,
  i18nProviders
} from '../../../../testing/unit-test-helper';
import { ErasureCodeProfileService } from '../../../shared/api/erasure-code-profile.service';
import { PoolModule } from '../pool.module';
import { CrushRulesetFormComponent } from './crush-ruleset-form.component';

describe('ErasureCodeProfileFormComponent', () => {
  let component: CrushRulesetFormComponent;
  let fixture: ComponentFixture<CrushRulesetFormComponent>;
  let fixtureHelper: FixtureHelper;

  configureTestBed({
    imports: [
      HttpClientTestingModule,
      RouterTestingModule,
      ToastrModule.forRoot(),
      PoolModule,
      NgBootstrapFormValidationModule.forRoot()
    ],
    providers: [ErasureCodeProfileService, BsModalRef, i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CrushRulesetFormComponent);
    fixtureHelper = new FixtureHelper(fixture);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

});
