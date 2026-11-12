import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { RouterTestingModule } from '@angular/router/testing';

import { SharedModule } from '~/app/shared/shared.module';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { Permission, Permissions } from '~/app/shared/models/permissions';
import { configureTestBed } from '~/testing/unit-test-helper';
import { DashboardHelpComponent } from './dashboard-help.component';

describe('DashboardHelpComponent', () => {
  let component: DashboardHelpComponent;
  let fixture: ComponentFixture<DashboardHelpComponent>;
  let permissions: Permissions;

  configureTestBed({
    imports: [HttpClientTestingModule, SharedModule, RouterTestingModule],
    declarations: [DashboardHelpComponent],
    providers: [AuthStorageService]
  });

  beforeEach(() => {
    permissions = new Permissions({});
    permissions.configOpt = new Permission(['read']);
    spyOn(TestBed.inject(AuthStorageService), 'getPermissions').and.returnValue(permissions);
    fixture = TestBed.createComponent(DashboardHelpComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should show report issue when config-opt is readable', () => {
    const options = fixture.debugElement.queryAll(By.css('cds-overflow-menu-option'));
    // About + Report an issue (Documentation and API are plain <li> links)
    expect(options.length).toBe(2);
    expect(options.map((o) => o.nativeElement.textContent.trim())).toEqual([
      'About',
      'Report an issue...'
    ]);
  });

  it('should hide report issue when config-opt is not readable', () => {
    permissions.configOpt = new Permission([]);
    (TestBed.inject(AuthStorageService).getPermissions as jasmine.Spy).and.returnValue(permissions);

    fixture = TestBed.createComponent(DashboardHelpComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();

    const options = fixture.debugElement.queryAll(By.css('cds-overflow-menu-option'));
    // Only About remains; Documentation and API are plain <li> links
    expect(options.length).toBe(1);
    expect(options[0].nativeElement.textContent.trim()).toBe('About');
  });
});
