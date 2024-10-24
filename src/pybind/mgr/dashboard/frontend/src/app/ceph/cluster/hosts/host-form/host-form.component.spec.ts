import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { ToastrModule } from 'ngx-toastr';

import { LoadingPanelComponent } from '~/app/shared/components/loading-panel/loading-panel.component';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed, FormHelper } from '~/testing/unit-test-helper';
import { HostFormComponent } from './host-form.component';
import { InputModule, ModalModule } from 'carbon-components-angular';

describe('HostFormComponent', () => {
  let component: HostFormComponent;
  let fixture: ComponentFixture<HostFormComponent>;
  let formHelper: FormHelper;

  configureTestBed(
    {
      imports: [
        SharedModule,
        HttpClientTestingModule,
        RouterTestingModule,
        ReactiveFormsModule,
        ToastrModule.forRoot(),
        InputModule,
        ModalModule
      ],
      declarations: [HostFormComponent],
      providers: [NgbActiveModal]
    },
    [LoadingPanelComponent]
  );

  beforeEach(() => {
    fixture = TestBed.createComponent(HostFormComponent);
    component = fixture.componentInstance;
    component.ngOnInit();
    formHelper = new FormHelper(component.hostForm);
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should open the form in a modal', () => {
    const nativeEl = fixture.debugElement.nativeElement;
    expect(nativeEl.querySelector('cds-modal')).not.toBe(null);
  });

  it('should validate the network address is valid', fakeAsync(() => {
    formHelper.setValue('addr', '115.42.150.37', true);
    tick();
    formHelper.expectValid('addr');
  }));

  it('should show error if network address is invalid', fakeAsync(() => {
    formHelper.setValue('addr', '666.10.10.20', true);
    tick();
    formHelper.expectError('addr', 'pattern');
  }));

  it('should submit the network address', () => {
    component.hostForm.get('addr').setValue('127.0.0.1');
    fixture.detectChanges();
    component.submit();
    expect(component.addr).toBe('127.0.0.1');
  });

  it('should validate the labels are added', () => {
    const labels = ['label1', 'label2'];
    component.hostForm.get('labels').patchValue(labels);
    fixture.detectChanges();
    component.submit();
    expect(component.allLabels).toBe(labels);
  });

  it('should select maintenance mode', () => {
    component.hostForm.get('maintenance').setValue(true);
    fixture.detectChanges();
    component.submit();
    expect(component.status).toBe('maintenance');
  });

  it('should expand the hostname correctly', () => {
    component.hostForm.get('hostname').setValue('ceph-node-00.cephlab.com');
    fixture.detectChanges();
    component.submit();
    expect(component.hostnameArray).toStrictEqual(['ceph-node-00.cephlab.com']);

    component.hostnameArray = [];

    component.hostForm.get('hostname').setValue('ceph-node-[00-10].cephlab.com');
    fixture.detectChanges();
    component.submit();
    expect(component.hostnameArray).toStrictEqual([
      'ceph-node-00.cephlab.com',
      'ceph-node-01.cephlab.com',
      'ceph-node-02.cephlab.com',
      'ceph-node-03.cephlab.com',
      'ceph-node-04.cephlab.com',
      'ceph-node-05.cephlab.com',
      'ceph-node-06.cephlab.com',
      'ceph-node-07.cephlab.com',
      'ceph-node-08.cephlab.com',
      'ceph-node-09.cephlab.com',
      'ceph-node-10.cephlab.com'
    ]);

    component.hostnameArray = [];

    component.hostForm.get('hostname').setValue('ceph-node-00.cephlab.com,ceph-node-1.cephlab.com');
    fixture.detectChanges();
    component.submit();
    expect(component.hostnameArray).toStrictEqual([
      'ceph-node-00.cephlab.com',
      'ceph-node-1.cephlab.com'
    ]);

    component.hostnameArray = [];

    component.hostForm
      .get('hostname')
      .setValue('ceph-mon-[01-05].lab.com,ceph-osd-[1-4].lab.com,ceph-rgw-[001-006].lab.com');
    fixture.detectChanges();
    component.submit();
    expect(component.hostnameArray).toStrictEqual([
      'ceph-mon-01.lab.com',
      'ceph-mon-02.lab.com',
      'ceph-mon-03.lab.com',
      'ceph-mon-04.lab.com',
      'ceph-mon-05.lab.com',
      'ceph-osd-1.lab.com',
      'ceph-osd-2.lab.com',
      'ceph-osd-3.lab.com',
      'ceph-osd-4.lab.com',
      'ceph-rgw-001.lab.com',
      'ceph-rgw-002.lab.com',
      'ceph-rgw-003.lab.com',
      'ceph-rgw-004.lab.com',
      'ceph-rgw-005.lab.com',
      'ceph-rgw-006.lab.com'
    ]);

    component.hostnameArray = [];

    component.hostForm
      .get('hostname')
      .setValue('ceph-(mon-[00-04],osd-[001-005],rgw-[1-3]).lab.com');
    fixture.detectChanges();
    component.submit();
    expect(component.hostnameArray).toStrictEqual([
      'ceph-mon-00.lab.com',
      'ceph-mon-01.lab.com',
      'ceph-mon-02.lab.com',
      'ceph-mon-03.lab.com',
      'ceph-mon-04.lab.com',
      'ceph-osd-001.lab.com',
      'ceph-osd-002.lab.com',
      'ceph-osd-003.lab.com',
      'ceph-osd-004.lab.com',
      'ceph-osd-005.lab.com',
      'ceph-rgw-1.lab.com',
      'ceph-rgw-2.lab.com',
      'ceph-rgw-3.lab.com'
    ]);
  });
});
