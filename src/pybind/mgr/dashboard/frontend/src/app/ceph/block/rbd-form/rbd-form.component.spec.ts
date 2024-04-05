import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { By } from '@angular/platform-browser';
import { ActivatedRoute, Router } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastrModule } from 'ngx-toastr';
import { NEVER, of } from 'rxjs';
import { delay } from 'rxjs/operators';

import { Pool } from '~/app/ceph/pool/pool';
import { PoolService } from '~/app/shared/api/pool.service';
import { RbdService } from '~/app/shared/api/rbd.service';
import { ImageSpec } from '~/app/shared/models/image-spec';
import { SharedModule } from '~/app/shared/shared.module';
import { ActivatedRouteStub } from '~/testing/activated-route-stub';
import { configureTestBed } from '~/testing/unit-test-helper';
import { RbdConfigurationFormComponent } from '../rbd-configuration-form/rbd-configuration-form.component';
import { RbdImageFeature } from './rbd-feature.interface';
import { RbdFormMode } from './rbd-form-mode.enum';
import { RbdFormResponseModel } from './rbd-form-response.model';
import { RbdFormComponent } from './rbd-form.component';

describe('RbdFormComponent', () => {
  const urlPrefix = {
    create: '/block/rbd/create',
    edit: '/block/rbd/edit',
    clone: '/block/rbd/clone',
    copy: '/block/rbd/copy'
  };
  let component: RbdFormComponent;
  let fixture: ComponentFixture<RbdFormComponent>;
  let activatedRoute: ActivatedRouteStub;
  const mock: { rbd: RbdFormResponseModel; pools: Pool[]; defaultFeatures: string[] } = {
    rbd: {} as RbdFormResponseModel,
    pools: [],
    defaultFeatures: []
  };

  const setRouterUrl = (
    action: 'create' | 'edit' | 'clone' | 'copy',
    poolName?: string,
    imageName?: string
  ) => {
    component['routerUrl'] = [urlPrefix[action], poolName, imageName].filter((x) => x).join('/');
  };

  const queryNativeElement = (cssSelector: string) =>
    fixture.debugElement.query(By.css(cssSelector)).nativeElement;

  configureTestBed({
    imports: [
      HttpClientTestingModule,
      ReactiveFormsModule,
      RouterTestingModule,
      ToastrModule.forRoot(),
      SharedModule
    ],
    declarations: [RbdFormComponent, RbdConfigurationFormComponent],
    providers: [
      {
        provide: ActivatedRoute,
        useValue: new ActivatedRouteStub({ pool: 'foo', name: 'bar', snap: undefined })
      },
      RbdService
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RbdFormComponent);
    component = fixture.componentInstance;
    activatedRoute = <ActivatedRouteStub>TestBed.inject(ActivatedRoute);

    component.loadingReady();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('create/edit/clone/copy image', () => {
    let createAction: jasmine.Spy;
    let editAction: jasmine.Spy;
    let cloneAction: jasmine.Spy;
    let copyAction: jasmine.Spy;
    let rbdServiceGetSpy: jasmine.Spy;
    let routerNavigate: jasmine.Spy;

    const DELAY = 100;

    const getPool = (
      pool_name: string,
      type: 'replicated' | 'erasure',
      flags_names: string,
      application_metadata: string[]
    ): Pool =>
      ({
        pool_name,
        flags_names,
        application_metadata,
        type
      } as Pool);

    beforeEach(() => {
      createAction = spyOn(component, 'createAction').and.returnValue(of(null));
      editAction = spyOn(component, 'editAction');
      editAction.and.returnValue(of(null));
      cloneAction = spyOn(component, 'cloneAction').and.returnValue(of(null));
      copyAction = spyOn(component, 'copyAction').and.returnValue(of(null));
      spyOn(component, 'setResponse').and.stub();
      routerNavigate = spyOn(TestBed.inject(Router), 'navigate').and.stub();
      mock.pools = [
        getPool('one', 'replicated', '', []),
        getPool('two', 'replicated', '', ['rbd']),
        getPool('three', 'replicated', '', ['rbd']),
        getPool('four', 'erasure', '', ['rbd']),
        getPool('four', 'erasure', 'ec_overwrites', ['rbd'])
      ];
      spyOn(TestBed.inject(PoolService), 'list').and.callFake(() => of(mock.pools));
      rbdServiceGetSpy = spyOn(TestBed.inject(RbdService), 'get');
      mock.rbd = ({ pool_name: 'foo', pool_image: 'bar' } as any) as RbdFormResponseModel;
      rbdServiceGetSpy.and.returnValue(of(mock.rbd));
      component.mode = undefined;
    });

    it('should create image', () => {
      component.ngOnInit();
      component.submit();

      expect(createAction).toHaveBeenCalledTimes(1);
      expect(editAction).toHaveBeenCalledTimes(0);
      expect(cloneAction).toHaveBeenCalledTimes(0);
      expect(copyAction).toHaveBeenCalledTimes(0);
      expect(routerNavigate).toHaveBeenCalledTimes(1);
    });

    it('should unsubscribe right after image data is received', () => {
      setRouterUrl('edit', 'foo', 'bar');
      rbdServiceGetSpy.and.returnValue(of(mock.rbd));
      editAction.and.returnValue(NEVER);
      expect(component['rbdImage'].observers.length).toEqual(0);
      component.ngOnInit(); // Subscribes to image once during init
      component.submit();
      expect(createAction).toHaveBeenCalledTimes(0);
      expect(editAction).toHaveBeenCalledTimes(1);
      expect(cloneAction).toHaveBeenCalledTimes(0);
      expect(copyAction).toHaveBeenCalledTimes(0);
      expect(routerNavigate).toHaveBeenCalledTimes(0);
    });

    it('should not edit image if no image data is received', fakeAsync(() => {
      setRouterUrl('edit', 'foo', 'bar');
      rbdServiceGetSpy.and.returnValue(of(mock.rbd).pipe(delay(DELAY)));
      component.ngOnInit();
      component.submit();

      expect(createAction).toHaveBeenCalledTimes(0);
      expect(editAction).toHaveBeenCalledTimes(0);
      expect(cloneAction).toHaveBeenCalledTimes(0);
      expect(copyAction).toHaveBeenCalledTimes(0);
      expect(routerNavigate).toHaveBeenCalledTimes(0);

      tick(DELAY);
    }));

    describe('disable data pools', () => {
      beforeEach(() => {
        component.ngOnInit();
      });

      it('should be enabled with more than 1 pool', () => {
        component['handleExternalData'](mock);
        expect(component.allDataPools.length).toBe(3);
        expect(component.rbdForm.get('useDataPool').disabled).toBe(false);

        mock.pools.pop();
        component['handleExternalData'](mock);
        expect(component.allDataPools.length).toBe(2);
        expect(component.rbdForm.get('useDataPool').disabled).toBe(false);
      });

      it('should be disabled with 1 pool', () => {
        mock.pools = [mock.pools[0]];
        component['handleExternalData'](mock);
        expect(component.rbdForm.get('useDataPool').disabled).toBe(true);
      });

      // Reason for 2 tests - useDataPool is not re-enabled anywhere else
      it('should be disabled without any pool', () => {
        mock.pools = [];
        component['handleExternalData'](mock);
        expect(component.rbdForm.get('useDataPool').disabled).toBe(true);
      });
    });

    it('should edit image after image data is received', () => {
      setRouterUrl('edit', 'foo', 'bar');
      component.ngOnInit();
      component.submit();

      expect(createAction).toHaveBeenCalledTimes(0);
      expect(editAction).toHaveBeenCalledTimes(1);
      expect(cloneAction).toHaveBeenCalledTimes(0);
      expect(copyAction).toHaveBeenCalledTimes(0);
      expect(routerNavigate).toHaveBeenCalledTimes(1);
    });

    it('should not clone image if no image data is received', fakeAsync(() => {
      setRouterUrl('clone', 'foo', 'bar');
      rbdServiceGetSpy.and.returnValue(of(mock.rbd).pipe(delay(DELAY)));
      component.ngOnInit();
      component.submit();

      expect(createAction).toHaveBeenCalledTimes(0);
      expect(editAction).toHaveBeenCalledTimes(0);
      expect(cloneAction).toHaveBeenCalledTimes(0);
      expect(copyAction).toHaveBeenCalledTimes(0);
      expect(routerNavigate).toHaveBeenCalledTimes(0);

      tick(DELAY);
    }));

    it('should clone image after image data is received', () => {
      setRouterUrl('clone', 'foo', 'bar');
      component.ngOnInit();
      component.submit();

      expect(createAction).toHaveBeenCalledTimes(0);
      expect(editAction).toHaveBeenCalledTimes(0);
      expect(cloneAction).toHaveBeenCalledTimes(1);
      expect(copyAction).toHaveBeenCalledTimes(0);
      expect(routerNavigate).toHaveBeenCalledTimes(1);
    });

    it('should not copy image if no image data is received', fakeAsync(() => {
      setRouterUrl('copy', 'foo', 'bar');
      rbdServiceGetSpy.and.returnValue(of(mock.rbd).pipe(delay(DELAY)));
      component.ngOnInit();
      component.submit();

      expect(createAction).toHaveBeenCalledTimes(0);
      expect(editAction).toHaveBeenCalledTimes(0);
      expect(cloneAction).toHaveBeenCalledTimes(0);
      expect(copyAction).toHaveBeenCalledTimes(0);
      expect(routerNavigate).toHaveBeenCalledTimes(0);

      tick(DELAY);
    }));

    it('should copy image after image data is received', () => {
      setRouterUrl('copy', 'foo', 'bar');
      component.ngOnInit();
      component.submit();

      expect(createAction).toHaveBeenCalledTimes(0);
      expect(editAction).toHaveBeenCalledTimes(0);
      expect(cloneAction).toHaveBeenCalledTimes(0);
      expect(copyAction).toHaveBeenCalledTimes(1);
      expect(routerNavigate).toHaveBeenCalledTimes(1);
    });
  });

  describe('should test decodeURIComponent of params', () => {
    let rbdService: RbdService;

    beforeEach(() => {
      rbdService = TestBed.inject(RbdService);
      component.mode = RbdFormMode.editing;
      fixture.detectChanges();
      spyOn(rbdService, 'get').and.callThrough();
    });

    it('with namespace', () => {
      activatedRoute.setParams({ image_spec: 'foo%2Fbar%2Fbaz' });

      expect(rbdService.get).toHaveBeenCalledWith(new ImageSpec('foo', 'bar', 'baz'));
    });

    it('without snapName', () => {
      activatedRoute.setParams({ image_spec: 'foo%2Fbar', snap: undefined });

      expect(rbdService.get).toHaveBeenCalledWith(new ImageSpec('foo', null, 'bar'));
      expect(component.snapName).toBeUndefined();
    });

    it('with snapName', () => {
      activatedRoute.setParams({ image_spec: 'foo%2Fbar', snap: 'baz%2Fbaz' });

      expect(rbdService.get).toHaveBeenCalledWith(new ImageSpec('foo', null, 'bar'));
      expect(component.snapName).toBe('baz/baz');
    });
  });

  describe('test image configuration component', () => {
    beforeEach(() => {
      fixture.detectChanges();
    });
    it('is hidden by default under Advanced', () => {
      fixture.detectChanges();
      expect(
        queryNativeElement('cd-rbd-configuration-form')
          .closest('.accordion-collapse')
          .classList.contains('show')
      ).toBeFalsy();
    });

    it('is visible when Advanced is not collapsed', () => {
      queryNativeElement('#advanced-fieldset').click();
      fixture.detectChanges();
      expect(
        queryNativeElement('cd-rbd-configuration-form').closest('.accordion-collapse').classList
      ).toContain('show');
    });
  });

  describe('tests for feature flags', () => {
    let deepFlatten: any, layering: any, exclusiveLock: any, objectMap: any, fastDiff: any;
    const defaultFeatures = [
      // Supposed to be enabled by default
      'deep-flatten',
      'exclusive-lock',
      'fast-diff',
      'layering',
      'object-map'
    ];
    const allFeatureNames = [
      'deep-flatten',
      'layering',
      'exclusive-lock',
      'object-map',
      'fast-diff'
    ];
    const setFeatures = (features: Record<string, RbdImageFeature>) => {
      component.features = features;
      component.featuresList = component.objToArray(features);
      component.createForm();
    };
    const getFeatureNativeElements = () => allFeatureNames.map((f) => queryNativeElement(`#${f}`));

    it('should convert feature flags correctly in the constructor', () => {
      setFeatures({
        one: { desc: 'one', allowEnable: true, allowDisable: true },
        two: { desc: 'two', allowEnable: true, allowDisable: true },
        three: { desc: 'three', allowEnable: true, allowDisable: true }
      });
      expect(component.featuresList).toEqual([
        { desc: 'one', key: 'one', allowDisable: true, allowEnable: true },
        { desc: 'two', key: 'two', allowDisable: true, allowEnable: true },
        { desc: 'three', key: 'three', allowDisable: true, allowEnable: true }
      ]);
    });

    describe('test edit form flags', () => {
      const prepare = (pool: string, image: string, enabledFeatures: string[]): void => {
        const rbdService = TestBed.inject(RbdService);
        spyOn(rbdService, 'get').and.returnValue(
          of({
            name: image,
            pool_name: pool,
            features_name: enabledFeatures
          })
        );
        spyOn(rbdService, 'defaultFeatures').and.returnValue(of(defaultFeatures));
        setRouterUrl('edit', pool, image);
        fixture.detectChanges();
        [deepFlatten, layering, exclusiveLock, objectMap, fastDiff] = getFeatureNativeElements();
      };

      it('should have the interlock feature for flags disabled, if one feature is not set', () => {
        prepare('rbd', 'foobar', ['deep-flatten', 'exclusive-lock', 'layering', 'object-map']);

        expect(objectMap.disabled).toBe(false);
        expect(fastDiff.disabled).toBe(false);

        expect(objectMap.checked).toBe(true);
        expect(fastDiff.checked).toBe(false);

        fastDiff.click();
        fastDiff.click();

        expect(objectMap.checked).toBe(true); // Shall not be disabled by `fast-diff`!
      });

      it('should not disable object-map when fast-diff is unchecked', () => {
        prepare('rbd', 'foobar', ['deep-flatten', 'exclusive-lock', 'layering', 'object-map']);

        fastDiff.click();
        fastDiff.click();

        expect(objectMap.checked).toBe(true); // Shall not be disabled by `fast-diff`!
      });

      it('should not enable fast-diff when object-map is checked', () => {
        prepare('rbd', 'foobar', ['deep-flatten', 'exclusive-lock', 'layering', 'object-map']);

        objectMap.click();
        objectMap.click();

        expect(fastDiff.checked).toBe(false); // Shall not be disabled by `fast-diff`!
      });
    });

    describe('test create form flags', () => {
      beforeEach(() => {
        const rbdService = TestBed.inject(RbdService);
        spyOn(rbdService, 'defaultFeatures').and.returnValue(of(defaultFeatures));
        setRouterUrl('create');
        fixture.detectChanges();
        [deepFlatten, layering, exclusiveLock, objectMap, fastDiff] = getFeatureNativeElements();
      });

      it('should initialize the checkboxes correctly', () => {
        expect(deepFlatten.disabled).toBe(false);
        expect(layering.disabled).toBe(false);
        expect(exclusiveLock.disabled).toBe(false);
        expect(objectMap.disabled).toBe(false);
        expect(fastDiff.disabled).toBe(false);

        expect(deepFlatten.checked).toBe(true);
        expect(layering.checked).toBe(true);
        expect(exclusiveLock.checked).toBe(true);
        expect(objectMap.checked).toBe(true);
        expect(fastDiff.checked).toBe(true);
      });

      it('should disable features if their requirements are not met (exclusive-lock)', () => {
        exclusiveLock.click(); // unchecks exclusive-lock
        expect(objectMap.disabled).toBe(true);
        expect(fastDiff.disabled).toBe(true);
      });

      it('should disable features if their requirements are not met (object-map)', () => {
        objectMap.click(); // unchecks object-map
        expect(fastDiff.disabled).toBe(true);
      });
    });

    describe('test mirroring options', () => {
      beforeEach(() => {
        component.ngOnInit();
        fixture.detectChanges();
        const mirroring = fixture.debugElement.query(By.css('#mirroring')).nativeElement;
        mirroring.click();
        fixture.detectChanges();
      });

      it('should verify two mirroring options are shown', () => {
        const journal = fixture.debugElement.query(By.css('#journal')).nativeElement;
        const snapshot = fixture.debugElement.query(By.css('#snapshot')).nativeElement;
        expect(journal).not.toBeNull();
        expect(snapshot).not.toBeNull();
      });

      it('should verify only snapshot is disabled for pools that are in pool mirror mode', () => {
        component.poolMirrorMode = 'pool';
        fixture.detectChanges();
        const journal = fixture.debugElement.query(By.css('#journal')).nativeElement;
        const snapshot = fixture.debugElement.query(By.css('#snapshot')).nativeElement;
        expect(journal.disabled).toBe(false);
        expect(snapshot.disabled).toBe(true);
      });

      it('should set and disable exclusive-lock only for the journal mode', () => {
        component.poolMirrorMode = 'pool';
        component.mirroring = true;
        const journal = fixture.debugElement.query(By.css('#journal')).nativeElement;
        journal.click();
        fixture.detectChanges();
        const exclusiveLocks = fixture.debugElement.query(By.css('#exclusive-lock')).nativeElement;
        expect(exclusiveLocks.checked).toBe(true);
        expect(exclusiveLocks.disabled).toBe(true);
      });

      it('should have journaling feature for journaling mirror mode on createRequest', () => {
        component.mirroring = true;
        fixture.detectChanges();
        const journal = fixture.debugElement.query(By.css('#journal')).nativeElement;
        journal.click();
        expect(journal.checked).toBe(true);
        const request = component.createRequest();
        expect(request.features).toContain('journaling');
      });

      it('should have journaling feature for journaling mirror mode on editRequest', () => {
        component.mirroring = true;
        fixture.detectChanges();
        const journal = fixture.debugElement.query(By.css('#journal')).nativeElement;
        journal.click();
        expect(journal.checked).toBe(true);
        const request = component.editRequest();
        expect(request.features).toContain('journaling');
      });
    });
  });
});
