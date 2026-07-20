import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';

import { Permission } from '~/app/shared/models/permissions';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { AdministrationComponent } from './administration.component';

describe('AdministrationComponent', () => {
  let component: AdministrationComponent;
  let fixture: ComponentFixture<AdministrationComponent>;

  configureTestBed({
    imports: [SharedModule],
    declarations: [AdministrationComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(AdministrationComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('settings icon visibility', () => {
    const setPermissions = (user: string[], configOpt: string[]) => {
      component.userPermission = new Permission(user);
      component.configOptPermission = new Permission(configOpt);
      fixture.detectChanges();
    };

    const settingsIcon = () => fixture.debugElement.query(By.css('[cdsIcon="settings"]'));

    it('should show the settings icon when user has user.read', () => {
      setPermissions(['read'], []);
      expect(settingsIcon()).toBeTruthy();
    });

    it('should show the settings icon when user has configOpt.read', () => {
      setPermissions([], ['read']);
      expect(settingsIcon()).toBeTruthy();
    });

    it('should show the settings icon when user has both user.read and configOpt.read', () => {
      setPermissions(['read'], ['read']);
      expect(settingsIcon()).toBeTruthy();
    });

    it('should hide the settings icon when user has neither user.read nor configOpt.read', () => {
      setPermissions([], []);
      expect(settingsIcon()).toBeFalsy();
    });
  });
});
