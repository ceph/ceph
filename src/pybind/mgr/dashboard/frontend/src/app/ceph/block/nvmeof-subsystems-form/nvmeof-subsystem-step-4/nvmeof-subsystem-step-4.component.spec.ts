import { ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastrModule } from 'ngx-toastr';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

import { SharedModule } from '~/app/shared/shared.module';
import { NvmeofSubsystemsStepFourComponent } from './nvmeof-subsystem-step-4.component';
import { GridModule } from 'carbon-components-angular';
import { AUTHENTICATION, HOST_TYPE } from '~/app/shared/models/nvmeof';

describe('NvmeofSubsystemsStepFourComponent', () => {
  let component: NvmeofSubsystemsStepFourComponent;
  let fixture: ComponentFixture<NvmeofSubsystemsStepFourComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [NvmeofSubsystemsStepFourComponent],
      providers: [NgbActiveModal],
      imports: [
        HttpClientTestingModule,
        ReactiveFormsModule,
        RouterTestingModule,
        SharedModule,
        GridModule,
        ToastrModule.forRoot()
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(NvmeofSubsystemsStepFourComponent);
    component = fixture.componentInstance;
    component.group = 'default';
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should have an empty formGroup', () => {
    expect(component.formGroup).toBeTruthy();
  });

  it('should return correct host access label for ALL hosts', () => {
    component.hostType = HOST_TYPE.ALL;
    expect(component.hostAccessLabel).toContain('All');
  });

  it('should return correct host access label for SPECIFIC hosts', () => {
    component.hostType = HOST_TYPE.SPECIFIC;
    expect(component.hostAccessLabel).toContain('Restricted');
  });

  it('should return correct auth type label', () => {
    component.authType = AUTHENTICATION.Bidirectional;
    expect(component.authTypeLabel).toContain('Bidirectional');

    component.authType = AUTHENTICATION.Unidirectional;
    expect(component.authTypeLabel).toContain('Unidirectional');
  });

  it('should return correct listener count', () => {
    component.listeners = [{ content: 'host1', addr: '1.2.3.4' }];
    expect(component.listenerCount).toBe(1);
  });

  it('should detect subsystem key presence', () => {
    component.subsystemDchapKey = '';
    expect(component.hasSubsystemKey).toBe(false);

    component.subsystemDchapKey = 'somekey';
    expect(component.hasSubsystemKey).toBe(true);
  });
});
