import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbActiveModal, NgbTooltipModule } from '@ng-bootstrap/ng-bootstrap';
import { ToastrModule } from 'ngx-toastr';
import { of as observableOf } from 'rxjs';

import { OsdService } from '~/app/shared/api/osd.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { Flag } from '~/app/shared/models/flag';
import { NotificationService } from '~/app/shared/services/notification.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { OsdFlagsIndivModalComponent } from './osd-flags-indiv-modal.component';

describe('OsdFlagsIndivModalComponent', () => {
  let component: OsdFlagsIndivModalComponent;
  let fixture: ComponentFixture<OsdFlagsIndivModalComponent>;
  let httpTesting: HttpTestingController;
  let osdService: OsdService;

  configureTestBed({
    imports: [
      HttpClientTestingModule,
      ReactiveFormsModule,
      SharedModule,
      ToastrModule.forRoot(),
      NgbTooltipModule,
      RouterTestingModule
    ],
    declarations: [OsdFlagsIndivModalComponent],
    providers: [NgbActiveModal]
  });

  beforeEach(() => {
    httpTesting = TestBed.inject(HttpTestingController);
    fixture = TestBed.createComponent(OsdFlagsIndivModalComponent);
    component = fixture.componentInstance;
    osdService = TestBed.inject(OsdService);
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('getActivatedIndivFlags', () => {
    function checkFlagsCount(
      counts: { [key: string]: number },
      expected: { [key: string]: number }
    ) {
      Object.entries(expected).forEach(([expectedKey, expectedValue]) => {
        expect(counts[expectedKey]).toBe(expectedValue);
      });
    }

    it('should count correctly if no flag has been set', () => {
      component.selected = generateSelected();
      const countedFlags = component.getActivatedIndivFlags();
      checkFlagsCount(countedFlags, { noup: 0, nodown: 0, noin: 0, noout: 0 });
    });

    it('should count correctly if some of the flags have been set', () => {
      component.selected = generateSelected([['noin'], ['noin', 'noout'], ['nodown']]);
      const countedFlags = component.getActivatedIndivFlags();
      checkFlagsCount(countedFlags, { noup: 0, nodown: 1, noin: 2, noout: 1 });
    });
  });

  describe('changeValue', () => {
    it('should change value correctly and set indeterminate to false', () => {
      const testFlag = component.flags[0];
      const value = testFlag.value;
      component.changeValue(testFlag);
      expect(testFlag.value).toBe(!value);
      expect(testFlag.indeterminate).toBeFalsy();
    });
  });

  describe('resetSelection', () => {
    it('should set a new flags object by deep cloning the initial selection', () => {
      component.resetSelection();
      expect(component.flags === component.initialSelection).toBeFalsy();
    });
  });

  describe('OSD single-select', () => {
    beforeEach(() => {
      component.selected = [{ osd: 0 }];
    });

    describe('ngOnInit', () => {
      it('should clone flags as initial selection', () => {
        expect(component.flags === component.initialSelection).toBeFalsy();
      });

      it('should initialize form correctly if no individual and global flags are set', () => {
        component.selected[0]['state'] = ['exists', 'up'];
        spyOn(osdService, 'getFlags').and.callFake(() => observableOf([]));
        fixture.detectChanges();
        checkFlags(component.flags);
      });

      it('should initialize form correctly if individual but no global flags are set', () => {
        component.selected[0]['state'] = ['exists', 'noout', 'up'];
        spyOn(osdService, 'getFlags').and.callFake(() => observableOf([]));
        fixture.detectChanges();
        const expected = {
          noout: { value: true, clusterWide: false, indeterminate: false }
        };
        checkFlags(component.flags, expected);
      });

      it('should initialize form correctly if multiple individual but no global flags are set', () => {
        component.selected[0]['state'] = ['exists', 'noin', 'noout', 'up'];
        spyOn(osdService, 'getFlags').and.callFake(() => observableOf([]));
        fixture.detectChanges();
        const expected = {
          noout: { value: true, clusterWide: false, indeterminate: false },
          noin: { value: true, clusterWide: false, indeterminate: false }
        };
        checkFlags(component.flags, expected);
      });

      it('should initialize form correctly if no individual but global flags are set', () => {
        component.selected[0]['state'] = ['exists', 'up'];
        spyOn(osdService, 'getFlags').and.callFake(() => observableOf(['noout']));
        fixture.detectChanges();
        const expected = {
          noout: { value: false, clusterWide: true, indeterminate: false }
        };
        checkFlags(component.flags, expected);
      });
    });

    describe('submitAction', () => {
      let notificationType: NotificationType;
      let notificationService: NotificationService;
      let bsModalRef: NgbActiveModal;
      let flags: object;

      beforeEach(() => {
        notificationService = TestBed.inject(NotificationService);
        spyOn(notificationService, 'show').and.callFake((type) => {
          notificationType = type;
        });
        bsModalRef = TestBed.inject(NgbActiveModal);
        spyOn(bsModalRef, 'close').and.callThrough();
        flags = {
          nodown: false,
          noin: false,
          noout: false,
          noup: false
        };
      });

      it('should submit an activated flag', () => {
        const code = component.flags[0].code;
        component.flags[0].value = true;
        component.submitAction();
        flags[code] = true;

        const req = httpTesting.expectOne('api/osd/flags/individual');
        req.flush({ flags, ids: [0] });
        expect(req.request.body).toEqual({ flags, ids: [0] });
        expect(notificationType).toBe(NotificationType.success);
        expect(component.activeModal.close).toHaveBeenCalledTimes(1);
      });

      it('should submit multiple flags', () => {
        const codes = [component.flags[0].code, component.flags[1].code];
        component.flags[0].value = true;
        component.flags[1].value = true;
        component.submitAction();
        flags[codes[0]] = true;
        flags[codes[1]] = true;

        const req = httpTesting.expectOne('api/osd/flags/individual');
        req.flush({ flags, ids: [0] });
        expect(req.request.body).toEqual({ flags, ids: [0] });
        expect(notificationType).toBe(NotificationType.success);
        expect(component.activeModal.close).toHaveBeenCalledTimes(1);
      });

      it('should hide modal if request fails', () => {
        component.flags = [];
        component.submitAction();
        const req = httpTesting.expectOne('api/osd/flags/individual');
        req.flush([], { status: 500, statusText: 'failure' });
        expect(notificationService.show).toHaveBeenCalledTimes(0);
        expect(component.activeModal.close).toHaveBeenCalledTimes(1);
      });
    });
  });

  describe('OSD multi-select', () => {
    describe('ngOnInit', () => {
      it('should initialize form correctly if same individual and no global flags are set', () => {
        component.selected = generateSelected([['noin'], ['noin'], ['noin']]);
        spyOn(osdService, 'getFlags').and.callFake(() => observableOf([]));
        fixture.detectChanges();
        const expected = {
          noin: { value: true, clusterWide: false, indeterminate: false }
        };
        checkFlags(component.flags, expected);
      });

      it('should initialize form correctly if different individual and no global flags are set', () => {
        component.selected = generateSelected([['noin'], ['noout'], ['noin']]);
        spyOn(osdService, 'getFlags').and.callFake(() => observableOf([]));
        fixture.detectChanges();
        const expected = {
          noin: { value: false, clusterWide: false, indeterminate: true },
          noout: { value: false, clusterWide: false, indeterminate: true }
        };
        checkFlags(component.flags, expected);
      });

      it('should initialize form correctly if different and same individual and no global flags are set', () => {
        component.selected = generateSelected([
          ['noin', 'nodown'],
          ['noout', 'nodown'],
          ['noin', 'nodown']
        ]);
        spyOn(osdService, 'getFlags').and.callFake(() => observableOf([]));
        fixture.detectChanges();
        const expected = {
          noin: { value: false, clusterWide: false, indeterminate: true },
          noout: { value: false, clusterWide: false, indeterminate: true },
          nodown: { value: true, clusterWide: false, indeterminate: false }
        };
        checkFlags(component.flags, expected);
      });

      it('should initialize form correctly if a flag is set for all OSDs individually and globally', () => {
        component.selected = generateSelected([
          ['noin', 'nodown'],
          ['noout', 'nodown'],
          ['noin', 'nodown']
        ]);
        spyOn(osdService, 'getFlags').and.callFake(() => observableOf(['noout']));
        fixture.detectChanges();
        const expected = {
          noin: { value: false, clusterWide: false, indeterminate: true },
          noout: { value: false, clusterWide: true, indeterminate: true },
          nodown: { value: true, clusterWide: false, indeterminate: false }
        };
        checkFlags(component.flags, expected);
      });

      it('should initialize form correctly if different individual and global flags are set', () => {
        component.selected = generateSelected([
          ['noin', 'nodown', 'noout'],
          ['noout', 'nodown'],
          ['noin', 'nodown', 'noout']
        ]);
        spyOn(osdService, 'getFlags').and.callFake(() => observableOf(['noout']));
        fixture.detectChanges();
        const expected = {
          noin: { value: false, clusterWide: false, indeterminate: true },
          noout: { value: true, clusterWide: true, indeterminate: false },
          nodown: { value: true, clusterWide: false, indeterminate: false }
        };
        checkFlags(component.flags, expected);
      });
    });

    describe('submitAction', () => {
      let notificationType: NotificationType;
      let notificationService: NotificationService;
      let bsModalRef: NgbActiveModal;
      let flags: object;

      beforeEach(() => {
        notificationService = TestBed.inject(NotificationService);
        spyOn(notificationService, 'show').and.callFake((type) => {
          notificationType = type;
        });
        bsModalRef = TestBed.inject(NgbActiveModal);
        spyOn(bsModalRef, 'close').and.callThrough();
        flags = {
          nodown: false,
          noin: false,
          noout: false,
          noup: false
        };
      });

      it('should submit an activated flag for multiple OSDs', () => {
        component.selected = generateSelected();
        const code = component.flags[0].code;
        const submittedIds = [0, 1, 2];
        component.flags[0].value = true;
        component.submitAction();
        flags[code] = true;

        const req = httpTesting.expectOne('api/osd/flags/individual');
        req.flush({ flags, ids: submittedIds });
        expect(req.request.body).toEqual({ flags, ids: submittedIds });
        expect(notificationType).toBe(NotificationType.success);
        expect(component.activeModal.close).toHaveBeenCalledTimes(1);
      });

      it('should submit multiple flags for multiple OSDs', () => {
        component.selected = generateSelected();
        const codes = [component.flags[0].code, component.flags[1].code];
        const submittedIds = [0, 1, 2];
        component.flags[0].value = true;
        component.flags[1].value = true;
        component.submitAction();
        flags[codes[0]] = true;
        flags[codes[1]] = true;

        const req = httpTesting.expectOne('api/osd/flags/individual');
        req.flush({ flags, ids: submittedIds });
        expect(req.request.body).toEqual({ flags, ids: submittedIds });
        expect(notificationType).toBe(NotificationType.success);
        expect(component.activeModal.close).toHaveBeenCalledTimes(1);
      });
    });
  });

  function checkFlags(flags: Flag[], expected: object = {}) {
    flags.forEach((flag) => {
      let value = false;
      let clusterWide = false;
      let indeterminate = false;
      if (Object.keys(expected).includes(flag.code)) {
        value = expected[flag.code]['value'];
        clusterWide = expected[flag.code]['clusterWide'];
        indeterminate = expected[flag.code]['indeterminate'];
      }
      expect(flag.value).toBe(value);
      expect(flag.clusterWide).toBe(clusterWide);
      expect(flag.indeterminate).toBe(indeterminate);
    });
  }

  function generateSelected(flags: string[][] = []) {
    const defaultFlags = ['exists', 'up'];
    const osds = [];
    const count = flags.length || 3;
    for (let i = 0; i < count; i++) {
      const osd = {
        osd: i,
        state: defaultFlags.concat(flags[i]) || defaultFlags
      };
      osds.push(osd);
    }
    return osds;
  }
});
