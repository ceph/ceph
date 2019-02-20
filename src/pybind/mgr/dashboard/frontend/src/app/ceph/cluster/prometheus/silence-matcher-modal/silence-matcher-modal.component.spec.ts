import { ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';

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

  it('should have a value field', () => {
    formH.expectError('value', 'required');
    formH.expectValidChange('value', 'avg(cpu_load[5m]) > avg(cpu_load[1d]) * 1.5');
  });

  it('should generate the right url for Prometheus', () => {
    const host = 'http://localhost:9090';
    const value = 'node_load1 > 1 and node_load1 > 3';
    const expected = host + '/api/v1/query?query=node_load1%20%3E%201%20and%20node_load1%20%3E%203';
    expect(component.generatePromQlURL(host, value)).toBe(expected);
  });

  describe('verifying', () => {
    it('should that the matcher combination matches something', () => {});
    it('should warn if it only matches something that is not active', () => {});
    it('should warn if it only matches active and non active alerts', () => {});
    it('should show matched alerts', () => {});
  });
});
