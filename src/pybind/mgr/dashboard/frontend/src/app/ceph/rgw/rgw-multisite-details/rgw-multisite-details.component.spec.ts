import { HttpClientTestingModule } from '@angular/common/http/testing';
import { DebugElement } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ToastrModule } from 'ngx-toastr';
import { SharedModule } from '~/app/shared/shared.module';

import { RgwMultisiteDetailsComponent } from './rgw-multisite-details.component';
import { RouterTestingModule } from '@angular/router/testing';
import { configureTestBed } from '~/testing/unit-test-helper';
import { NgbNavModule, NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { RgwMultisiteRealmFormComponent } from '../rgw-multisite-realm-form/rgw-multisite-realm-form.component';

describe('RgwMultisiteDetailsComponent', () => {
  let component: RgwMultisiteDetailsComponent;
  let fixture: ComponentFixture<RgwMultisiteDetailsComponent>;
  let debugElement: DebugElement;
  let modalService: ModalCdsService;

  configureTestBed({
    declarations: [RgwMultisiteDetailsComponent],
    imports: [
      HttpClientTestingModule,
      SharedModule,
      ToastrModule.forRoot(),
      RouterTestingModule,
      NgbNavModule
    ],
    providers: [NgbActiveModal]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwMultisiteDetailsComponent);
    component = fixture.componentInstance;
    debugElement = fixture.debugElement;
    modalService = TestBed.inject(ModalCdsService);
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should display right title', () => {
    const span = debugElement.nativeElement.querySelector('.card-header');
    expect(span.textContent.trim()).toBe('Topology Viewer');
  });

  it('should call openModal to open realm modal', () => {
    const entity = { data: { type: 'realm' } };
    const edit = true;

    spyOn(modalService, 'show');

    component.openModal(entity, edit);

    expect(modalService.show).toHaveBeenCalledWith(RgwMultisiteRealmFormComponent, {
      resource: 'realm',
      action: component.actionLabels.EDIT,
      info: entity,
      defaultsInfo: component.defaultsInfo,
      multisiteInfo: component.multisiteInfo
    });
  });
});
