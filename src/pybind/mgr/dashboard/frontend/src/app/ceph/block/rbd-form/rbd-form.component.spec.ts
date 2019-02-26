import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { ActivatedRoute } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';
import { TooltipModule } from 'ngx-bootstrap/tooltip';

import { ToastModule } from 'ng2-toastr';

import { By } from '@angular/platform-browser';
import { ActivatedRouteStub } from '../../../../testing/activated-route-stub';
import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { RbdService } from '../../../shared/api/rbd.service';
import { SharedModule } from '../../../shared/shared.module';
import { RbdConfigurationFormComponent } from '../rbd-configuration-form/rbd-configuration-form.component';
import { RbdFormMode } from './rbd-form-mode.enum';
import { RbdFormComponent } from './rbd-form.component';

describe('RbdFormComponent', () => {
  let component: RbdFormComponent;
  let fixture: ComponentFixture<RbdFormComponent>;
  let activatedRoute: ActivatedRouteStub;

  configureTestBed({
    imports: [
      HttpClientTestingModule,
      ReactiveFormsModule,
      RouterTestingModule,
      ToastModule.forRoot(),
      SharedModule,
      TooltipModule
    ],
    declarations: [RbdFormComponent, RbdConfigurationFormComponent],
    providers: [
      {
        provide: ActivatedRoute,
        useValue: new ActivatedRouteStub({ pool: 'foo', name: 'bar', snap: undefined })
      },
      i18nProviders
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RbdFormComponent);
    component = fixture.componentInstance;
    activatedRoute = TestBed.get(ActivatedRoute);
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('should test decodeURIComponent of params', () => {
    let rbdService: RbdService;

    beforeEach(() => {
      rbdService = TestBed.get(RbdService);
      component.mode = RbdFormMode.editing;
      fixture.detectChanges();
      spyOn(rbdService, 'get').and.callThrough();
    });

    it('without snapName', () => {
      activatedRoute.setParams({ pool: 'foo%2Ffoo', name: 'bar%2Fbar', snap: undefined });

      expect(rbdService.get).toHaveBeenCalledWith('foo/foo', 'bar/bar');
      expect(component.snapName).toBeUndefined();
    });

    it('with snapName', () => {
      activatedRoute.setParams({ pool: 'foo%2Ffoo', name: 'bar%2Fbar', snap: 'baz%2Fbaz' });

      expect(rbdService.get).toHaveBeenCalledWith('foo/foo', 'bar/bar');
      expect(component.snapName).toBe('baz/baz');
    });
  });

  describe('test image configuration component', () => {
    it('is visible', () => {
      fixture.detectChanges();
      expect(
        fixture.debugElement.query(By.css('cd-rbd-configuration-form')).nativeElement.parentElement
          .hidden
      ).toBe(false);
    });
  });
});
