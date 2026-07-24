import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { RouterTestingModule } from '@angular/router/testing';

import { Permission, Permissions } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { AdministrationComponent } from './administration.component';

describe('AdministrationComponent', () => {
  let component: AdministrationComponent;
  let fixture: ComponentFixture<AdministrationComponent>;
  let permissions: Permissions;

  const createComponent = () => {
    spyOn(TestBed.inject(AuthStorageService), 'getPermissions').and.returnValue(permissions);
    fixture = TestBed.createComponent(AdministrationComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  };

  configureTestBed({
    imports: [SharedModule, RouterTestingModule],
    declarations: [AdministrationComponent],
    providers: [AuthStorageService]
  });

  beforeEach(() => {
    permissions = new Permissions({});
  });

  it('should create', () => {
    createComponent();
    expect(component).toBeTruthy();
  });

  describe('Settings icon visibility', () => {
    /**
     * The settings icon is inside <ng-template #customTrigger> which is not
     * projected into the DOM by the CDS component in JSDOM. We therefore
     * verify the guard condition through the component's own Permission objects,
     * mirroring the *ngIf expression in the template:
     *   userPermission.create || userPermission.update || userPermission.delete || configOptPermission.read
     */
    const iconVisible = (c: AdministrationComponent) =>
      c['userPermission'].create ||
      c['userPermission'].update ||
      c['userPermission'].delete ||
      c['configOptPermission'].read;

    it('should hide the settings icon for a read-only user', () => {
      permissions.user = new Permission(['read']);
      permissions.configOpt = new Permission([]);
      createComponent();
      expect(iconVisible(component)).toBeFalsy();
    });

    it('should show the settings icon when user has create permission', () => {
      permissions.user = new Permission(['read', 'create']);
      permissions.configOpt = new Permission([]);
      createComponent();
      expect(iconVisible(component)).toBeTruthy();
    });

    it('should show the settings icon when user has update permission', () => {
      permissions.user = new Permission(['read', 'update']);
      permissions.configOpt = new Permission([]);
      createComponent();
      expect(iconVisible(component)).toBeTruthy();
    });

    it('should show the settings icon when user has delete permission', () => {
      permissions.user = new Permission(['read', 'delete']);
      permissions.configOpt = new Permission([]);
      createComponent();
      expect(iconVisible(component)).toBeTruthy();
    });

    it('should show the settings icon when configOpt is readable (even with read-only user)', () => {
      permissions.user = new Permission(['read']);
      permissions.configOpt = new Permission(['read']);
      createComponent();
      expect(iconVisible(component)).toBeTruthy();
    });
  });

  describe('User Management menu item visibility', () => {
    it('should hide User Management for a read-only user', () => {
      permissions.user = new Permission(['read']);
      createComponent();
      const buttons = fixture.debugElement.queryAll(By.css('.cds--overflow-menu-options__btn'));
      const labels = buttons.map((b) => b.nativeElement.textContent.trim());
      expect(labels).not.toContain('User management');
    });

    it('should show User Management when user has create permission', () => {
      permissions.user = new Permission(['read', 'create']);
      createComponent();
      const buttons = fixture.debugElement.queryAll(By.css('.cds--overflow-menu-options__btn'));
      const labels = buttons.map((b) => b.nativeElement.textContent.trim());
      expect(labels).toContain('User management');
    });
  });

  describe('Telemetry menu item visibility', () => {
    it('should hide Telemetry configuration when configOpt is not readable', () => {
      permissions.configOpt = new Permission([]);
      createComponent();
      const buttons = fixture.debugElement.queryAll(By.css('.cds--overflow-menu-options__btn'));
      const labels = buttons.map((b) => b.nativeElement.textContent.trim());
      expect(labels).not.toContain('Telemetry configuration');
    });

    it('should show Telemetry configuration when configOpt is readable', () => {
      permissions.configOpt = new Permission(['read']);
      createComponent();
      const buttons = fixture.debugElement.queryAll(By.css('.cds--overflow-menu-options__btn'));
      const labels = buttons.map((b) => b.nativeElement.textContent.trim());
      expect(labels).toContain('Telemetry configuration');
    });
  });
});
