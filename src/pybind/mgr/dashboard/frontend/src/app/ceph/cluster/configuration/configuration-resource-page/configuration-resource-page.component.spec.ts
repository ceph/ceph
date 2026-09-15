import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ActivatedRoute } from '@angular/router';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ReplaySubject } from 'rxjs';

import { configureTestBed } from '~/testing/unit-test-helper';
import { ConfigurationResourcePageComponent } from './configuration-resource-page.component';
import {
  ConfigurationOption,
  ConfigurationResourceStateService
} from '~/app/shared/services/configuration-resource-state.service';

describe('ConfigurationResourcePageComponent', () => {
  let component: ConfigurationResourcePageComponent;
  let fixture: ComponentFixture<ConfigurationResourcePageComponent>;
  let configurationSubject: ReplaySubject<ConfigurationOption | null>;

  const configurationServiceStateMock = {
    configuration$: new ReplaySubject<ConfigurationOption | null>(1)
  };

  const activatedRouteMock = {
    snapshot: {
      data: { section: 'overview' }
    }
  };

  configureTestBed({
    declarations: [ConfigurationResourcePageComponent],
    providers: [
      { provide: ActivatedRoute, useValue: activatedRouteMock },
      { provide: ConfigurationResourceStateService, useValue: configurationServiceStateMock }
    ],
    schemas: [NO_ERRORS_SCHEMA]
  });

  beforeEach(() => {
    configurationSubject = configurationServiceStateMock.configuration$;
    configurationSubject.next({
      name: 'mon_allow_pool_delete',
      desc: 'Allow pool delete',
      long_desc: 'Option for allowing pool delete operations',
      value: [{ section: 'global', value: 'false' }],
      default: 'false',
      daemon_default: 'false',
      min: '',
      max: '',
      type: 'bool',
      flags: ['runtime'],
      services: 'mon,mgr',
      can_update_at_runtime: true
    });

    fixture = TestBed.createComponent(ConfigurationResourcePageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should initialize section from route data', () => {
    expect(component.section).toBe('overview');
  });

  it('should load configuration details and build overview fields', () => {
    expect(component.selection?.name).toBe('mon_allow_pool_delete');
    expect(component.overviewFields.length).toBeGreaterThan(0);
  });

  it('should set notFound when configuration is null', () => {
    configurationSubject.next(null);
    fixture.detectChanges();

    expect(component.notFound).toBe(true);
    expect(component.selection).toBeUndefined();
  });
});
