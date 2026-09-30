import { ComponentFixture, TestBed } from '@angular/core/testing';
import { NvmeofNamespaceExpandModalComponent } from './nvmeof-namespace-expand-modal.component';
import { ActivatedRoute } from '@angular/router';
import { ActivatedRouteStub } from '~/testing/activated-route-stub';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { SharedModule } from '~/app/shared/shared.module';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastrModule } from 'ngx-toastr';
import { ModalModule, NumberModule } from 'carbon-components-angular';
import { of } from 'rxjs';
import { configureTestBed } from '~/testing/unit-test-helper';
import { FormatterService } from '~/app/shared/services/formatter.service';

describe('NvmeofNamespaceExpandModalComponent', () => {
  let component: NvmeofNamespaceExpandModalComponent;
  let fixture: ComponentFixture<NvmeofNamespaceExpandModalComponent>;
  let nvmeofService: NvmeofService;

  const mockNvmeofService = {
    getNamespace: () =>
      of({
        nsid: '1',
        rbd_pool_name: 'pool1',
        rbd_image_name: 'image1',
        rbd_image_size: new FormatterService().toBytes('1GiB'),
        block_size: 4096,
        rw_ios_per_second: 0,
        rw_mbytes_per_second: 0,
        r_mbytes_per_second: 0,
        w_mbytes_per_second: 0
      }),
    updateNamespace: () => of({})
  };

  const activatedRouteStub = new ActivatedRouteStub(
    { subsystem_nqn: 'nqn.2014-08.org.nvmexpress:uuid:12345', nsid: '1' },
    { group: 'group1' }
  );
  // Mock the parent route for relative navigation
  Object.defineProperty(activatedRouteStub, 'parent', { get: () => ({}) });

  configureTestBed({
    declarations: [NvmeofNamespaceExpandModalComponent],
    imports: [
      HttpClientTestingModule,
      SharedModule,
      ReactiveFormsModule,
      RouterTestingModule,
      ToastrModule.forRoot(),
      ModalModule,
      NumberModule
    ],
    providers: [
      { provide: NvmeofService, useValue: mockNvmeofService },
      { provide: ActivatedRoute, useValue: activatedRouteStub }
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(NvmeofNamespaceExpandModalComponent);
    component = fixture.componentInstance;
    nvmeofService = TestBed.inject(NvmeofService);

    // params are already set in constructor of stub above

    spyOn(nvmeofService, 'getNamespace').and.callThrough();
    spyOn(nvmeofService, 'updateNamespace').and.callThrough();

    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should initialize form with existing data', () => {
    expect(component.nsForm.get('image_size').value).toBe(1);
  });

  it('should validate size - error if smaller', () => {
    component.nsForm.get('image_size').setValue(0.5);
    component.nsForm.get('image_size').updateValueAndValidity();
    expect(component.nsForm.get('image_size').hasError('minSize')).toBe(true);
  });

  it('should validate size - success if larger', () => {
    component.nsForm.get('image_size').setValue(2);
    component.nsForm.get('image_size').updateValueAndValidity();
    expect(component.nsForm.get('image_size').hasError('minSize')).toBe(false);
  });
});
