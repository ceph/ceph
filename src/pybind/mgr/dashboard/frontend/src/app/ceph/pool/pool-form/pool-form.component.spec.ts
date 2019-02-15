import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { AbstractControl } from '@angular/forms';
import { By } from '@angular/platform-browser';
import { ActivatedRoute, Router, Routes } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastModule } from 'ng2-toastr';
import { BsModalService } from 'ngx-bootstrap/modal';
import { TabsModule } from 'ngx-bootstrap/tabs';
import { of } from 'rxjs';

import { configureTestBed, FormHelper, i18nProviders } from '../../../../testing/unit-test-helper';
import { NotFoundComponent } from '../../../core/not-found/not-found.component';
import { ErasureCodeProfileService } from '../../../shared/api/erasure-code-profile.service';
import { PoolService } from '../../../shared/api/pool.service';
import { CriticalConfirmationModalComponent } from '../../../shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { SelectBadgesComponent } from '../../../shared/components/select-badges/select-badges.component';
import { CdFormGroup } from '../../../shared/forms/cd-form-group';
import { CrushRule } from '../../../shared/models/crush-rule';
import { ErasureCodeProfile } from '../../../shared/models/erasure-code-profile';
import { Permission } from '../../../shared/models/permissions';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { TaskWrapperService } from '../../../shared/services/task-wrapper.service';
import { Pool } from '../pool';
import { PoolModule } from '../pool.module';
import { PoolFormComponent } from './pool-form.component';

describe('PoolFormComponent', () => {
  const OSDS = 8;
  let formHelper: FormHelper;
  let component: PoolFormComponent;
  let fixture: ComponentFixture<PoolFormComponent>;
  let poolService: PoolService;
  let form: CdFormGroup;
  let router: Router;
  let ecpService: ErasureCodeProfileService;

  const setPgNum = (pgs): AbstractControl => {
    formHelper.setValue('poolType', 'erasure');
    const control = formHelper.setValue('pgNum', pgs);
    fixture.detectChanges();
    fixture.debugElement.query(By.css('#pgNum')).nativeElement.dispatchEvent(new Event('blur'));
    return control;
  };

  const createCrushRule = ({
    id = 0,
    name = 'somePoolName',
    min = 1,
    max = 10,
    type = 'replicated'
  }: {
    max?: number;
    min?: number;
    id?: number;
    name?: string;
    type?: string;
  }) => {
    const typeNumber = type === 'erasure' ? 3 : 1;
    const rule = new CrushRule();
    rule.max_size = max;
    rule.min_size = min;
    rule.rule_id = id;
    rule.ruleset = typeNumber;
    rule.rule_name = name;
    rule.steps = [
      {
        item_name: 'default',
        item: -1,
        op: 'take'
      },
      {
        num: 0,
        type: 'osd',
        op: 'choose_firstn'
      },
      {
        op: 'emit'
      }
    ];
    component.info['crush_rules_' + type].push(rule);
    return rule;
  };

  const testSubmit = (pool: any, taskName: string, poolServiceMethod: 'create' | 'update') => {
    spyOn(poolService, poolServiceMethod).and.stub();
    const taskWrapper = TestBed.get(TaskWrapperService);
    spyOn(taskWrapper, 'wrapTaskAroundCall').and.callThrough();
    component.submit();
    expect(poolService[poolServiceMethod]).toHaveBeenCalledWith(pool);
    expect(taskWrapper.wrapTaskAroundCall).toHaveBeenCalledWith({
      task: {
        name: taskName,
        metadata: {
          pool_name: pool.pool
        }
      },
      call: undefined // because of stub
    });
  };

  const setUpPoolComponent = () => {
    fixture = TestBed.createComponent(PoolFormComponent);
    component = fixture.componentInstance;
    component.info = {
      pool_names: [],
      osd_count: OSDS,
      is_all_bluestore: true,
      bluestore_compression_algorithm: 'snappy',
      compression_algorithms: ['snappy'],
      compression_modes: ['none', 'passive'],
      crush_rules_replicated: [],
      crush_rules_erasure: []
    };
    const ecp1 = new ErasureCodeProfile();
    ecp1.name = 'ecp1';
    component.ecProfiles = [ecp1];
    form = component.form;
    formHelper = new FormHelper(form);
  };

  const routes: Routes = [{ path: '404', component: NotFoundComponent }];

  configureTestBed({
    declarations: [NotFoundComponent],
    imports: [
      HttpClientTestingModule,
      RouterTestingModule.withRoutes(routes),
      ToastModule.forRoot(),
      TabsModule.forRoot(),
      PoolModule
    ],
    providers: [
      ErasureCodeProfileService,
      SelectBadgesComponent,
      { provide: ActivatedRoute, useValue: { params: of({ name: 'somePoolName' }) } },
      i18nProviders
    ]
  });

  beforeEach(() => {
    setUpPoolComponent();
    poolService = TestBed.get(PoolService);
    spyOn(poolService, 'getInfo').and.callFake(() => [component.info]);
    ecpService = TestBed.get(ErasureCodeProfileService);
    spyOn(ecpService, 'list').and.callFake(() => [component.ecProfiles]);
    router = TestBed.get(Router);
    spyOn(router, 'navigate').and.stub();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('redirect not allowed users', () => {
    let poolPermissions: Permission;
    let authStorageService: AuthStorageService;

    const testForRedirect = (times: number) => {
      component.authenticate();
      expect(router.navigate).toHaveBeenCalledTimes(times);
    };

    beforeEach(() => {
      poolPermissions = {
        create: false,
        update: false,
        read: false,
        delete: false
      };
      authStorageService = TestBed.get(AuthStorageService);
      spyOn(authStorageService, 'getPermissions').and.callFake(() => ({
        pool: poolPermissions
      }));
    });

    it('navigates to 404 if not allowed', () => {
      component.authenticate();
      expect(router.navigate).toHaveBeenCalledWith(['/404']);
    });

    it('navigates if user is not allowed', () => {
      testForRedirect(1);
      poolPermissions.read = true;
      testForRedirect(2);
      poolPermissions.delete = true;
      testForRedirect(3);
      poolPermissions.update = true;
      testForRedirect(4);
      component.editing = true;
      poolPermissions.update = false;
      poolPermissions.create = true;
      testForRedirect(5);
    });

    it('does not navigate users with right permissions', () => {
      poolPermissions.read = true;
      poolPermissions.create = true;
      testForRedirect(0);
      component.editing = true;
      poolPermissions.update = true;
      testForRedirect(0);
      poolPermissions.create = false;
      testForRedirect(0);
    });
  });

  describe('pool form validation', () => {
    beforeEach(() => {
      component.ngOnInit();
    });

    it('is invalid at the beginning all sub forms are valid', () => {
      expect(form.valid).toBeFalsy();
      ['name', 'poolType', 'pgNum'].forEach((name) => formHelper.expectError(name, 'required'));
      ['crushRule', 'size', 'erasureProfile', 'ecOverwrites'].forEach((name) =>
        formHelper.expectValid(name)
      );
      expect(component.form.get('compression').valid).toBeTruthy();
    });

    it('validates name', () => {
      expect(component.editing).toBeFalsy();
      formHelper.expectError('name', 'required');
      formHelper.expectValidChange('name', 'some-name');
      component.info.pool_names.push('someExistingPoolName');
      formHelper.expectErrorChange('name', 'someExistingPoolName', 'uniqueName');
      formHelper.expectErrorChange('name', 'wrong format with spaces', 'pattern');
    });

    it('should validate with dots in pool name', () => {
      formHelper.expectValidChange('name', 'pool.default.bar', true);
    });

    it('validates poolType', () => {
      formHelper.expectError('poolType', 'required');
      formHelper.expectValidChange('poolType', 'erasure');
      formHelper.expectValidChange('poolType', 'replicated');
    });

    it('validates pgNum in creation mode', () => {
      formHelper.expectError(form.get('pgNum'), 'required');
      formHelper.setValue('poolType', 'erasure');
      formHelper.expectValid(setPgNum(-28));
      expect(form.getValue('pgNum')).toBe(1);
      formHelper.expectValid(setPgNum(15));
      expect(form.getValue('pgNum')).toBe(16);
    });

    it('increases pgNum by the power of two for if the value has changed by one', () => {
      setPgNum('16');
      expect(setPgNum(17).value).toBe(32);
      expect(setPgNum(31).value).toBe(16);
    });

    it('not increases pgNum by more than one but lower than the next pg update change', () => {
      setPgNum('16');
      expect(setPgNum('18').value).toBe(16);
      expect(setPgNum('14').value).toBe(16);
    });

    it('validates pgNum in edit mode', () => {
      component.data.pool = new Pool('test');
      component.data.pool.pg_num = 16;
      component.editing = true;
      component.ngOnInit();
      formHelper.expectError(setPgNum('8'), 'noDecrease');
    });

    it('is valid if pgNum, poolType and name are valid', () => {
      formHelper.setValue('name', 'some-name');
      formHelper.setValue('poolType', 'erasure');
      setPgNum(1);
      expect(form.valid).toBeTruthy();
    });

    it('validates crushRule', () => {
      formHelper.expectValid('crushRule');
      formHelper.expectErrorChange('crushRule', { min_size: 20 }, 'tooFewOsds');
    });

    it('validates size', () => {
      formHelper.setValue('poolType', 'replicated');
      formHelper.expectValid('size');
      formHelper.setValue('crushRule', {
        min_size: 2,
        max_size: 6
      });
      formHelper.expectErrorChange('size', 1, 'min');
      formHelper.expectErrorChange('size', 8, 'max');
      formHelper.expectValidChange('size', 6);
    });

    it('validates compression mode default value', () => {
      expect(form.getValue('mode')).toBe('none');
    });

    describe('compression form', () => {
      beforeEach(() => {
        formHelper.setValue('poolType', 'replicated');
        formHelper.setValue('mode', 'passive');
      });

      it('is valid', () => {
        expect(component.form.get('compression').valid).toBeTruthy();
      });

      it('validates minBlobSize to be only valid between 0 and maxBlobSize', () => {
        formHelper.expectErrorChange('minBlobSize', -1, 'min');
        formHelper.expectValidChange('minBlobSize', 0);
        formHelper.setValue('maxBlobSize', '2 KiB');
        formHelper.expectErrorChange('minBlobSize', '3 KiB', 'maximum');
        formHelper.expectValidChange('minBlobSize', '1.9 KiB');
      });

      it('validates minBlobSize converts numbers', () => {
        const control = formHelper.setValue('minBlobSize', '1');
        fixture.detectChanges();
        formHelper.expectValid(control);
        expect(control.value).toBe('1 KiB');
      });

      it('validates maxBlobSize to be only valid bigger than minBlobSize', () => {
        formHelper.expectErrorChange('maxBlobSize', -1, 'min');
        formHelper.setValue('minBlobSize', '1 KiB');
        formHelper.expectErrorChange('maxBlobSize', '0.5 KiB', 'minimum');
        formHelper.expectValidChange('maxBlobSize', '1.5 KiB');
      });

      it('s valid to only use one blob size', () => {
        formHelper.expectValid(formHelper.setValue('minBlobSize', '1 KiB'));
        formHelper.expectValid(formHelper.setValue('maxBlobSize', ''));
        formHelper.expectValid(formHelper.setValue('minBlobSize', ''));
        formHelper.expectValid(formHelper.setValue('maxBlobSize', '1 KiB'));
      });

      it('dismisses any size error if one of the blob sizes is changed into a valid state', () => {
        const min = formHelper.setValue('minBlobSize', '10 KiB');
        const max = formHelper.setValue('maxBlobSize', '1 KiB');
        fixture.detectChanges();
        max.setValue('');
        formHelper.expectValid(min);
        formHelper.expectValid(max);
        max.setValue('1 KiB');
        fixture.detectChanges();
        min.setValue('0.5 KiB');
        formHelper.expectValid(min);
        formHelper.expectValid(max);
      });

      it('validates maxBlobSize converts numbers', () => {
        const control = formHelper.setValue('maxBlobSize', '2');
        fixture.detectChanges();
        expect(control.value).toBe('2 KiB');
      });

      it('validates that odd size validator works as expected', () => {
        const odd = (min, max) => component['oddBlobSize'](min, max);
        expect(odd('10', '8')).toBe(true);
        expect(odd('8', '-')).toBe(false);
        expect(odd('8', '10')).toBe(false);
        expect(odd(null, '8')).toBe(false);
        expect(odd('10', '')).toBe(false);
        expect(odd('10', null)).toBe(false);
        expect(odd(null, null)).toBe(false);
      });

      it('validates ratio to be only valid between 0 and 1', () => {
        formHelper.expectValid('ratio');
        formHelper.expectErrorChange('ratio', -0.1, 'min');
        formHelper.expectValidChange('ratio', 0);
        formHelper.expectValidChange('ratio', 1);
        formHelper.expectErrorChange('ratio', 1.1, 'max');
      });
    });

    it('validates application metadata name', () => {
      formHelper.setValue('poolType', 'replicated');
      fixture.detectChanges();
      const selectBadges = fixture.debugElement.query(By.directive(SelectBadgesComponent))
        .componentInstance;
      const control = selectBadges.cdSelect.filter;
      formHelper.expectValid(control);
      control.setValue('?');
      formHelper.expectError(control, 'pattern');
      control.setValue('Ab3_');
      formHelper.expectValid(control);
      control.setValue('a'.repeat(129));
      formHelper.expectError(control, 'maxlength');
    });
  });

  describe('pool type changes', () => {
    beforeEach(() => {
      component.ngOnInit();
      createCrushRule({ id: 3, min: 1, max: 1, name: 'ep1', type: 'erasure' });
      createCrushRule({ id: 0, min: 2, max: 4, name: 'rep1', type: 'replicated' });
      createCrushRule({ id: 1, min: 3, max: 18, name: 'rep2', type: 'replicated' });
    });

    it('should have a default replicated size of 3', () => {
      formHelper.setValue('poolType', 'replicated');
      expect(form.getValue('size')).toBe(3);
    });

    describe('replicatedRuleChange', () => {
      beforeEach(() => {
        formHelper.setValue('poolType', 'replicated');
        formHelper.setValue('size', 99);
      });

      it('should not set size if a replicated pool is not set', () => {
        formHelper.setValue('poolType', 'erasure');
        expect(form.getValue('size')).toBe(99);
        formHelper.setValue('crushRule', component.info.crush_rules_replicated[1]);
        expect(form.getValue('size')).toBe(99);
      });

      it('should set size to maximum if size exceeds maximum', () => {
        formHelper.setValue('crushRule', component.info.crush_rules_replicated[0]);
        expect(form.getValue('size')).toBe(4);
      });

      it('should set size to minimum if size is lower than minimum', () => {
        formHelper.setValue('size', -1);
        formHelper.setValue('crushRule', component.info.crush_rules_replicated[0]);
        expect(form.getValue('size')).toBe(2);
      });
    });

    describe('rulesChange', () => {
      it('has no effect if info is not there', () => {
        delete component.info;
        formHelper.setValue('poolType', 'replicated');
        expect(component.current.rules).toEqual([]);
      });

      it('has no effect if pool type is not set', () => {
        component['rulesChange']();
        expect(component.current.rules).toEqual([]);
      });

      it('shows all replicated rules when pool type is "replicated"', () => {
        formHelper.setValue('poolType', 'replicated');
        expect(component.current.rules).toEqual(component.info.crush_rules_replicated);
        expect(component.current.rules.length).toBe(2);
      });

      it('shows all erasure code rules when pool type is "erasure"', () => {
        formHelper.setValue('poolType', 'erasure');
        expect(component.current.rules).toEqual(component.info.crush_rules_erasure);
        expect(component.current.rules.length).toBe(1);
      });

      it('disables rule field if only one rule exists which is used in the disabled field', () => {
        formHelper.setValue('poolType', 'erasure');
        const control = form.get('crushRule');
        expect(control.value).toEqual(component.info.crush_rules_erasure[0]);
        expect(control.disabled).toBe(true);
      });

      it('does not select the first rule if more than one exist', () => {
        formHelper.setValue('poolType', 'replicated');
        const control = form.get('crushRule');
        expect(control.value).toEqual(null);
        expect(control.disabled).toBe(false);
      });

      it('changing between both types will not leave crushRule in a bad state', () => {
        formHelper.setValue('poolType', 'erasure');
        formHelper.setValue('poolType', 'replicated');
        const control = form.get('crushRule');
        expect(control.value).toEqual(null);
        expect(control.disabled).toBe(false);
        formHelper.setValue('poolType', 'erasure');
        expect(control.value).toEqual(component.info.crush_rules_erasure[0]);
        expect(control.disabled).toBe(true);
      });
    });
  });

  describe('getMaxSize and getMinSize', () => {
    const setCrushRule = ({ min, max }: { min?: number; max?: number }) => {
      formHelper.setValue('crushRule', {
        min_size: min,
        max_size: max
      });
    };

    it('returns nothing if osd count is 0', () => {
      component.info.osd_count = 0;
      expect(component.getMinSize()).toBe(undefined);
      expect(component.getMaxSize()).toBe(undefined);
    });

    it('returns nothing if info is not there', () => {
      delete component.info;
      expect(component.getMinSize()).toBe(undefined);
      expect(component.getMaxSize()).toBe(undefined);
    });

    it('returns minimum and maximum of rule', () => {
      setCrushRule({ min: 2, max: 6 });
      expect(component.getMinSize()).toBe(2);
      expect(component.getMaxSize()).toBe(6);
    });

    it('returns 1 as minimum and the osd count as maximum if no crush rule is available', () => {
      expect(component.getMinSize()).toBe(1);
      expect(component.getMaxSize()).toBe(OSDS);
    });

    it('returns the osd count as maximum if the rule maximum exceeds it', () => {
      setCrushRule({ max: 100 });
      expect(component.getMaxSize()).toBe(OSDS);
    });

    it('should return the osd count as minimum if its lower the the rule minimum', () => {
      setCrushRule({ min: 10 });
      expect(component.getMinSize()).toBe(10);
      const control = form.get('crushRule');
      expect(control.invalid).toBe(true);
      formHelper.expectError(control, 'tooFewOsds');
    });
  });

  describe('application metadata', () => {
    let selectBadges: SelectBadgesComponent;

    const testAddApp = (app?: string, result?: string[]) => {
      selectBadges.cdSelect.filter.setValue(app);
      selectBadges.cdSelect.updateFilter();
      selectBadges.cdSelect.selectOption();
      expect(component.data.applications.selected).toEqual(result);
    };

    const testRemoveApp = (app: string, result: string[]) => {
      selectBadges.cdSelect.removeItem(app);
      expect(component.data.applications.selected).toEqual(result);
    };

    const setCurrentApps = (apps: string[]) => {
      component.data.applications.selected = apps;
      fixture.detectChanges();
      selectBadges.cdSelect.ngOnInit();
      return apps;
    };

    beforeEach(() => {
      formHelper.setValue('poolType', 'replicated');
      fixture.detectChanges();
      selectBadges = fixture.debugElement.query(By.directive(SelectBadgesComponent))
        .componentInstance;
    });

    it('adds all predefined and a custom applications to the application metadata array', () => {
      testAddApp('g', ['rgw']);
      testAddApp('b', ['rbd', 'rgw']);
      testAddApp('c', ['cephfs', 'rbd', 'rgw']);
      testAddApp('something', ['cephfs', 'rbd', 'rgw', 'something']);
    });

    it('only allows 4 apps to be added to the array', () => {
      const apps = setCurrentApps(['d', 'c', 'b', 'a']);
      testAddApp('e', apps);
    });

    it('can remove apps', () => {
      setCurrentApps(['a', 'b', 'c', 'd']);
      testRemoveApp('c', ['a', 'b', 'd']);
      testRemoveApp('a', ['b', 'd']);
      testRemoveApp('d', ['b']);
      testRemoveApp('b', []);
    });

    it('does not remove any app that is not in the array', () => {
      const apps = ['a', 'b', 'c', 'd'];
      setCurrentApps(apps);
      testRemoveApp('e', apps);
      testRemoveApp('0', apps);
    });
  });

  describe('pg number changes', () => {
    beforeEach(() => {
      formHelper.setValue('crushRule', {
        min_size: 1,
        max_size: 20
      });
      component.ngOnInit();
      // triggers pgUpdate
      setPgNum(256);
    });

    describe('pgCalc', () => {
      const PGS = 1;

      const getValidCase = () => ({
        type: 'replicated',
        osds: OSDS,
        size: 4,
        ecp: {
          k: 2,
          m: 2
        },
        expected: 256
      });

      const testPgCalc = ({ type, osds, size, ecp, expected }) => {
        component.info.osd_count = osds;
        formHelper.setValue('poolType', type);
        if (type === 'replicated') {
          formHelper.setValue('size', size);
        } else {
          formHelper.setValue('erasureProfile', ecp);
        }
        expect(form.getValue('pgNum')).toBe(expected);
        expect(component.externalPgChange).toBe(PGS !== expected);
      };

      beforeEach(() => {
        setPgNum(PGS);
      });

      it('does not change anything if type is not valid', () => {
        const test = getValidCase();
        test.type = '';
        test.expected = PGS;
        testPgCalc(test);
      });

      it('does not change anything if ecp is not valid', () => {
        const test = getValidCase();
        test.expected = PGS;
        test.type = 'erasure';
        test.ecp = null;
        testPgCalc(test);
      });

      it('calculates some replicated values', () => {
        const test = getValidCase();
        testPgCalc(test);
        test.osds = 16;
        test.expected = 512;
        testPgCalc(test);
        test.osds = 8;
        test.size = 8;
        test.expected = 128;
        testPgCalc(test);
      });

      it('calculates erasure code values even if selection is disabled', () => {
        component['initEcp']([{ k: 2, m: 2, name: 'bla', plugin: '', technique: '' }]);
        const test = getValidCase();
        test.type = 'erasure';
        testPgCalc(test);
        expect(form.get('erasureProfile').disabled).toBeTruthy();
      });

      it('calculates some erasure code values', () => {
        const test = getValidCase();
        test.type = 'erasure';
        testPgCalc(test);
        test.osds = 16;
        test.ecp.m = 5;
        test.expected = 256;
        testPgCalc(test);
        test.ecp.k = 5;
        test.expected = 128;
        testPgCalc(test);
      });

      it('should not change a manual set pg number', () => {
        form.get('pgNum').markAsDirty();
        const test = getValidCase();
        test.expected = PGS;
        testPgCalc(test);
      });
    });

    describe('pgUpdate', () => {
      const testPgUpdate = (pgs, jump, returnValue) => {
        component['pgUpdate'](pgs, jump);
        expect(form.getValue('pgNum')).toBe(returnValue);
      };

      it('updates by value', () => {
        testPgUpdate(10, undefined, 8);
        testPgUpdate(22, undefined, 16);
        testPgUpdate(26, undefined, 32);
      });

      it('updates by jump -> a magnitude of the power of 2', () => {
        testPgUpdate(undefined, 1, 512);
        testPgUpdate(undefined, -1, 256);
        testPgUpdate(undefined, -2, 64);
        testPgUpdate(undefined, -10, 1);
      });

      it('returns 1 as minimum for false numbers', () => {
        testPgUpdate(-26, undefined, 1);
        testPgUpdate(0, undefined, 1);
        testPgUpdate(undefined, -20, 1);
      });

      it('uses by value and jump', () => {
        testPgUpdate(330, 0, 256);
        testPgUpdate(230, 2, 1024);
        testPgUpdate(230, 3, 2048);
      });
    });

    describe('pgKeyUp', () => {
      const testPgKeyUp = (keyName, returnValue) => {
        component.pgKeyUp({ key: keyName });
        expect(form.getValue('pgNum')).toBe(returnValue);
      };

      it('does nothing with unrelated keys', () => {
        testPgKeyUp('0', 256);
        testPgKeyUp(',', 256);
        testPgKeyUp('a', 256);
        testPgKeyUp('Space', 256);
        testPgKeyUp('ArrowLeft', 256);
        testPgKeyUp('ArrowRight', 256);
      });

      it('increments by jump with plus or ArrowUp', () => {
        testPgKeyUp('ArrowUp', 512);
        testPgKeyUp('ArrowUp', 1024);
        testPgKeyUp('+', 2048);
        testPgKeyUp('+', 4096);
      });

      it('decrement by jump with minus or ArrowDown', () => {
        testPgKeyUp('ArrowDown', 128);
        testPgKeyUp('ArrowDown', 64);
        testPgKeyUp('-', 32);
        testPgKeyUp('-', 16);
      });
    });
  });

  describe('crushRule', () => {
    beforeEach(() => {
      createCrushRule({ name: 'replicatedRule' });
      fixture.detectChanges();
      formHelper.setValue('poolType', 'replicated');
      fixture.detectChanges();
    });

    it('should not show info per default', () => {
      formHelper.expectElementVisible(fixture, '#crushRule', true);
      formHelper.expectElementVisible(fixture, '#crush-info-block', false);
    });

    it('should show info if the info button is clicked', () => {
      fixture.detectChanges();
      const infoButton = fixture.debugElement.query(By.css('#crush-info-button'));
      infoButton.triggerEventHandler('click', null);
      expect(component.data.crushInfo).toBeTruthy();
      fixture.detectChanges();
      expect(infoButton.classes['active']).toBeTruthy();
      formHelper.expectIdElementsVisible(fixture, ['crushRule', 'crush-info-block'], true);
    });
  });

  describe('erasure code profile', () => {
    const setSelectedEcp = (name: string) => {
      formHelper.setValue('erasureProfile', { name: name });
    };

    beforeEach(() => {
      formHelper.setValue('poolType', 'erasure');
      fixture.detectChanges();
    });

    it('should not show info per default', () => {
      formHelper.expectElementVisible(fixture, '#erasureProfile', true);
      formHelper.expectElementVisible(fixture, '#ecp-info-block', false);
    });

    it('should show info if the info button is clicked', () => {
      const infoButton = fixture.debugElement.query(By.css('#ecp-info-button'));
      infoButton.triggerEventHandler('click', null);
      expect(component.data.erasureInfo).toBeTruthy();
      fixture.detectChanges();
      expect(infoButton.classes['active']).toBeTruthy();
      formHelper.expectIdElementsVisible(fixture, ['erasureProfile', 'ecp-info-block'], true);
    });

    describe('ecp deletion', () => {
      let taskWrapper: TaskWrapperService;
      let deletion: CriticalConfirmationModalComponent;

      const callDeletion = () => {
        component.deleteErasureCodeProfile();
        deletion.submitActionObservable();
      };

      const testPoolDeletion = (name) => {
        setSelectedEcp(name);
        callDeletion();
        expect(ecpService.delete).toHaveBeenCalledWith(name);
        expect(taskWrapper.wrapTaskAroundCall).toHaveBeenCalledWith({
          task: {
            name: 'ecp/delete',
            metadata: {
              name: name
            }
          },
          call: undefined // because of stub
        });
      };

      beforeEach(() => {
        spyOn(TestBed.get(BsModalService), 'show').and.callFake((deletionClass, config) => {
          deletion = Object.assign(new deletionClass(), config.initialState);
          return {
            content: deletion
          };
        });
        spyOn(ecpService, 'delete').and.stub();
        taskWrapper = TestBed.get(TaskWrapperService);
        spyOn(taskWrapper, 'wrapTaskAroundCall').and.callThrough();
      });

      it('should delete two different erasure code profiles', () => {
        testPoolDeletion('someEcpName');
        testPoolDeletion('aDifferentEcpName');
      });
    });
  });

  describe('submit - create', () => {
    const setMultipleValues = (settings: {}) => {
      Object.keys(settings).forEach((name) => {
        formHelper.setValue(name, settings[name]);
      });
    };
    const testCreate = (pool) => {
      testSubmit(pool, 'pool/create', 'create');
    };

    beforeEach(() => {
      createCrushRule({ name: 'replicatedRule' });
      createCrushRule({ name: 'erasureRule', type: 'erasure', id: 1 });
    });

    describe('erasure coded pool', () => {
      it('minimum requirements', () => {
        setMultipleValues({
          name: 'minECPool',
          poolType: 'erasure',
          pgNum: 4
        });
        testCreate({
          pool: 'minECPool',
          pool_type: 'erasure',
          pg_num: 4
        });
      });

      it('with erasure coded profile', () => {
        const ecp = { name: 'ecpMinimalMock' };
        setMultipleValues({
          name: 'ecpPool',
          poolType: 'erasure',
          pgNum: 16,
          size: 2, // Will be ignored
          erasureProfile: ecp
        });
        testCreate({
          pool: 'ecpPool',
          pool_type: 'erasure',
          pg_num: 16,
          erasure_code_profile: ecp.name
        });
      });

      it('with ec_overwrite flag', () => {
        setMultipleValues({
          name: 'ecOverwrites',
          poolType: 'erasure',
          pgNum: 32,
          ecOverwrites: true
        });
        testCreate({
          pool: 'ecOverwrites',
          pool_type: 'erasure',
          pg_num: 32,
          flags: ['ec_overwrites']
        });
      });
    });

    describe('replicated coded pool', () => {
      it('minimum requirements', () => {
        const ecp = { name: 'ecpMinimalMock' };
        setMultipleValues({
          name: 'minRepPool',
          poolType: 'replicated',
          size: 2,
          erasureProfile: ecp, // Will be ignored
          pgNum: 8
        });
        testCreate({
          pool: 'minRepPool',
          pool_type: 'replicated',
          pg_num: 8,
          size: 2
        });
      });
    });

    it('pool with compression', () => {
      setMultipleValues({
        name: 'compression',
        poolType: 'erasure',
        pgNum: 64,
        mode: 'passive',
        algorithm: 'lz4',
        minBlobSize: '4 K',
        maxBlobSize: '4 M',
        ratio: 0.7
      });
      testCreate({
        pool: 'compression',
        pool_type: 'erasure',
        pg_num: 64,
        compression_mode: 'passive',
        compression_algorithm: 'lz4',
        compression_min_blob_size: 4096,
        compression_max_blob_size: 4194304,
        compression_required_ratio: 0.7
      });
    });

    it('pool with application metadata', () => {
      setMultipleValues({
        name: 'apps',
        poolType: 'erasure',
        pgNum: 128
      });
      component.data.applications.selected = ['cephfs', 'rgw'];
      testCreate({
        pool: 'apps',
        pool_type: 'erasure',
        pg_num: 128,
        application_metadata: ['cephfs', 'rgw']
      });
    });
  });

  describe('edit mode', () => {
    const setUrl = (url) => {
      Object.defineProperty(router, 'url', { value: url });
      setUpPoolComponent(); // Renew of component needed because the constructor has to be called
    };

    let pool: Pool;
    beforeEach(() => {
      pool = new Pool('somePoolName');
      pool.type = 'replicated';
      pool.size = 3;
      pool.crush_rule = 'someRule';
      pool.pg_num = 32;
      pool.options = {};
      pool.options.compression_mode = 'passive';
      pool.options.compression_algorithm = 'lz4';
      pool.options.compression_min_blob_size = 1024 * 512;
      pool.options.compression_max_blob_size = 1024 * 1024;
      pool.options.compression_required_ratio = 0.8;
      pool.flags_names = 'someFlag1,someFlag2';
      pool.application_metadata = ['rbd', 'rgw'];
      createCrushRule({ name: 'someRule' });
      spyOn(poolService, 'get').and.callFake(() => of(pool));
    });

    it('is not in edit mode if edit is not included in url', () => {
      setUrl('/pool/add');
      expect(component.editing).toBeFalsy();
    });

    it('is in edit mode if edit is included in url', () => {
      setUrl('/pool/edit/somePoolName');
      expect(component.editing).toBeTruthy();
    });

    describe('after ngOnInit', () => {
      beforeEach(() => {
        component.editing = true;
        component.ngOnInit();
      });

      it('disabled inputs', () => {
        const disabled = ['poolType', 'crushRule', 'size', 'erasureProfile', 'ecOverwrites'];
        disabled.forEach((controlName) => {
          return expect(form.get(controlName).disabled).toBeTruthy();
        });
        const enabled = [
          'name',
          'pgNum',
          'mode',
          'algorithm',
          'minBlobSize',
          'maxBlobSize',
          'ratio'
        ];
        enabled.forEach((controlName) => {
          return expect(form.get(controlName).enabled).toBeTruthy();
        });
      });

      it('set all control values to the given pool', () => {
        expect(form.getValue('name')).toBe(pool.pool_name);
        expect(form.getValue('poolType')).toBe(pool.type);
        expect(form.getValue('crushRule')).toEqual(component.info.crush_rules_replicated[0]);
        expect(form.getValue('size')).toBe(pool.size);
        expect(form.getValue('pgNum')).toBe(pool.pg_num);
        expect(form.getValue('mode')).toBe(pool.options.compression_mode);
        expect(form.getValue('algorithm')).toBe(pool.options.compression_algorithm);
        expect(form.getValue('minBlobSize')).toBe('512 KiB');
        expect(form.getValue('maxBlobSize')).toBe('1 MiB');
        expect(form.getValue('ratio')).toBe(pool.options.compression_required_ratio);
      });

      it('is only be possible to use the same or more pgs like before', () => {
        formHelper.expectValid(setPgNum(64));
        formHelper.expectError(setPgNum(4), 'noDecrease');
      });

      describe('submit', () => {
        const markControlAsPreviouslySet = (controlName) => form.get(controlName).markAsPristine();

        beforeEach(() => {
          ['algorithm', 'maxBlobSize', 'minBlobSize', 'mode', 'pgNum', 'ratio', 'name'].forEach(
            (name) => markControlAsPreviouslySet(name)
          );
          fixture.detectChanges();
        });

        it(`always provides the application metadata array with submit even if it's empty`, () => {
          expect(form.get('mode').dirty).toBe(false);
          component.data.applications.selected = [];
          testSubmit(
            {
              application_metadata: [],
              pool: 'somePoolName'
            },
            'pool/edit',
            'update'
          );
        });

        it(`will always provide reset value for compression options`, () => {
          formHelper.setValue('minBlobSize', '').markAsDirty();
          formHelper.setValue('maxBlobSize', '').markAsDirty();
          formHelper.setValue('ratio', '').markAsDirty();
          testSubmit(
            {
              application_metadata: ['rbd', 'rgw'],
              compression_max_blob_size: 0,
              compression_min_blob_size: 0,
              compression_required_ratio: 0,
              pool: 'somePoolName'
            },
            'pool/edit',
            'update'
          );
        });

        it(`will unset mode not used anymore`, () => {
          formHelper.setValue('mode', 'none').markAsDirty();
          testSubmit(
            {
              application_metadata: ['rbd', 'rgw'],
              compression_mode: 'unset',
              pool: 'somePoolName'
            },
            'pool/edit',
            'update'
          );
        });
      });
    });
  });
});
