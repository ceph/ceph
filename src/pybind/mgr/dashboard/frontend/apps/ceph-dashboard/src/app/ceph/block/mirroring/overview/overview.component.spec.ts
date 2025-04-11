import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbNavModule, NgbProgressbarModule } from '@ng-bootstrap/ng-bootstrap';
import { ToastrModule } from 'ngx-toastr';
import { of } from 'rxjs';

import { RbdMirroringService } from '~/app/shared/api/rbd-mirroring.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { DaemonListComponent } from '../daemon-list/daemon-list.component';
import { ImageListComponent } from '../image-list/image-list.component';
import { MirrorHealthColorPipe } from '../mirror-health-color.pipe';
import { PoolListComponent } from '../pool-list/pool-list.component';
import { OverviewComponent } from './overview.component';
import { ButtonModule, GridModule, InputModule } from 'carbon-components-angular';

describe('OverviewComponent', () => {
  let component: OverviewComponent;
  let fixture: ComponentFixture<OverviewComponent>;
  let rbdMirroringService: RbdMirroringService;

  configureTestBed({
    declarations: [
      DaemonListComponent,
      ImageListComponent,
      MirrorHealthColorPipe,
      OverviewComponent,
      PoolListComponent
    ],
    imports: [
      BrowserAnimationsModule,
      SharedModule,
      NgbNavModule,
      NgbProgressbarModule,
      HttpClientTestingModule,
      RouterTestingModule,
      ReactiveFormsModule,
      ToastrModule.forRoot(),
      ButtonModule,
      InputModule,
      GridModule
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(OverviewComponent);
    component = fixture.componentInstance;
    rbdMirroringService = TestBed.inject(RbdMirroringService);
    component.siteName = 'site-A';
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('edit site name', () => {
    beforeEach(() => {
      spyOn(rbdMirroringService, 'getSiteName').and.callFake(() => of({ site_name: 'site-A' }));
      spyOn(rbdMirroringService, 'refresh').and.stub();
      fixture.detectChanges();
    });

    afterEach(() => {
      expect(rbdMirroringService.refresh).toHaveBeenCalledTimes(1);
    });

    it('should call setSiteName', () => {
      component.editing = true;
      spyOn(rbdMirroringService, 'setSiteName').and.callFake(() => of({ site_name: 'new-site-A' }));

      component.rbdmirroringForm.patchValue({
        siteName: 'new-site-A'
      });
      component.updateSiteName();
      expect(rbdMirroringService.setSiteName).toHaveBeenCalledWith('new-site-A');
    });
  });
});
