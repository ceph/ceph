import { HttpClientTestingModule } from '@angular/common/http/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { By } from '@angular/platform-browser';

import { CheckboxModule, ModalModule } from 'carbon-components-angular';
import { of, throwError } from 'rxjs';

import { ConfigurationService } from '~/app/shared/api/configuration.service';
import { OsdService } from '~/app/shared/api/osd.service';
import { PoolMigrationClusterSettingsModalComponent } from './pool-migration-cluster-settings-modal.component';

describe('PoolMigrationClusterSettingsModalComponent', () => {
  let component: PoolMigrationClusterSettingsModalComponent;
  let fixture: ComponentFixture<PoolMigrationClusterSettingsModalComponent>;
  let configurationService: ConfigurationService;
  let osdService: OsdService;
  let onComplete: jest.Mock;

  const createComponent = (
    needsFlag: boolean,
    needsMinCompatClient: boolean,
    flagValue: string = ''
  ) => {
    onComplete = jest.fn();
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule, ReactiveFormsModule, ModalModule, CheckboxModule],
      declarations: [PoolMigrationClusterSettingsModalComponent],
      providers: [
        { provide: 'needsFlag', useValue: needsFlag },
        { provide: 'needsMinCompatClient', useValue: needsMinCompatClient },
        { provide: 'flagValue', useValue: flagValue },
        { provide: 'onComplete', useValue: onComplete }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    });
    fixture = TestBed.createComponent(PoolMigrationClusterSettingsModalComponent);
    component = fixture.componentInstance;
    configurationService = TestBed.inject(ConfigurationService);
    osdService = TestBed.inject(OsdService);
    fixture.detectChanges();
  };

  const description = () =>
    fixture.debugElement.query(By.css('[data-testid="description"]')).nativeElement.textContent;
  const checkbox = () => fixture.debugElement.query(By.css('[data-testid="understand-checkbox"]'));

  describe('when both settings are missing', () => {
    beforeEach(() => createComponent(true, true, 'other'));

    it('should mention both in the description', () => {
      expect(description()).toContain('pool migration capability enabled');
      expect(description()).toContain('compatible umbrella client');
    });

    it('should require acknowledging the irreversible change', () => {
      expect(checkbox()).not.toBeNull();
      expect(component.form.invalid).toBe(true);
      component.form.get('understand').setValue(true);
      expect(component.form.valid).toBe(true);
    });

    it('should enable the flag keeping existing features and set the min compat client', () => {
      const create = jest.spyOn(configurationService, 'create').mockReturnValue(of(null));
      const setMin = jest.spyOn(osdService, 'setMinCompatClient').mockReturnValue(of(null));
      const closeSpy = jest.spyOn(component, 'closeModal');

      component.onSubmit();

      expect(create).toHaveBeenCalledWith({
        name: 'enable_experimental_unrecoverable_data_corrupting_features',
        value: [{ section: 'global', value: 'other,poolmigration' }],
        force_update: false
      });
      expect(setMin).toHaveBeenCalledWith('umbrella');
      expect(closeSpy).toHaveBeenCalled();
      expect(onComplete).toHaveBeenCalled();
    });

    it('should keep the modal open and not continue when a request fails', () => {
      jest.spyOn(configurationService, 'create').mockReturnValue(of(null));
      jest.spyOn(osdService, 'setMinCompatClient').mockReturnValue(throwError(() => 'fail'));
      const closeSpy = jest.spyOn(component, 'closeModal');

      component.onSubmit();

      expect(closeSpy).not.toHaveBeenCalled();
      expect(onComplete).not.toHaveBeenCalled();
      expect(component.form.errors).toEqual({ cdSubmitButton: true });
    });
  });

  describe('when only the experimental flag is missing', () => {
    beforeEach(() => createComponent(true, false));

    it('should only talk about the pool migration capability', () => {
      expect(description()).toContain('pool migration capability enabled');
      expect(description()).not.toContain('umbrella');
    });

    it('should not show the acknowledgement and be submittable right away', () => {
      expect(checkbox()).toBeNull();
      expect(component.form.valid).toBe(true);
    });

    it('should only update the flag', () => {
      const create = jest.spyOn(configurationService, 'create').mockReturnValue(of(null));
      const setMin = jest.spyOn(osdService, 'setMinCompatClient');

      component.onSubmit();

      expect(create).toHaveBeenCalledWith(
        expect.objectContaining({ value: [{ section: 'global', value: 'poolmigration' }] })
      );
      expect(setMin).not.toHaveBeenCalled();
      expect(onComplete).toHaveBeenCalled();
    });
  });

  describe('when only the minimum compatible client is missing', () => {
    beforeEach(() => createComponent(false, true));

    it('should only talk about the umbrella client', () => {
      expect(description()).toContain('compatible umbrella client');
      expect(description()).not.toContain('pool migration capability');
    });

    it('should require the acknowledgement', () => {
      expect(checkbox()).not.toBeNull();
      expect(component.form.invalid).toBe(true);
    });

    it('should only set the min compat client', () => {
      const create = jest.spyOn(configurationService, 'create');
      const setMin = jest.spyOn(osdService, 'setMinCompatClient').mockReturnValue(of(null));

      component.onSubmit();

      expect(create).not.toHaveBeenCalled();
      expect(setMin).toHaveBeenCalledWith('umbrella');
      expect(onComplete).toHaveBeenCalled();
    });
  });
});
