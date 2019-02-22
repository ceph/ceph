import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { BsModalRef } from 'ngx-bootstrap/modal';

import {
  configureTestBed,
  FixtureHelper,
  FormHelper,
  i18nProviders
} from '../../../../../testing/unit-test-helper';
import { SharedModule } from '../../../../shared/shared.module';
import { ClusterModule } from '../../cluster.module';
import { SilenceMatcherModalComponent } from './silence-matcher-modal.component';

describe('SilenceMatcherModalComponent', () => {
  let component: SilenceMatcherModalComponent;
  let fixture: ComponentFixture<SilenceMatcherModalComponent>;
  let formH: FormHelper;
  let fixtureH: FixtureHelper;

  configureTestBed({
    imports: [HttpClientTestingModule, SharedModule, ClusterModule],
    providers: [BsModalRef, i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(SilenceMatcherModalComponent);
    fixtureH = new FixtureHelper(fixture);
    component = fixture.componentInstance;
    formH = new FormHelper(component.form);
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should have a name field', () => {
    formH.expectError('name', 'required');
    formH.expectValidChange('name', 'CPU 50% above usual load');
  });

  it('should contain a alert name list based on rules from prometheus', () => {});

  it('should contain a alert severity list based on rules from prometheus', () => {});

  it('matches value with name against alert list', () => {});

  it('should have a value field', () => {
    formH.expectError('value', 'required');
    formH.expectValidChange('value', 'avg(cpu_load[5m]) > avg(cpu_load[1d]) * 1.5');
  });

  describe('verifying', () => {
    it('should that the matcher combination matches something', () => {});
    it('should warn if it only matches something that is not active', () => {});
    it('should warn if it only matches active and non active alerts', () => {});
    it('should show matched alerts', () => {});
  });
});
