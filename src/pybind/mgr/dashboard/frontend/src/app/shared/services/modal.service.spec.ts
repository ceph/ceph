import { Component } from '@angular/core';
import { fakeAsync, TestBed, tick } from '@angular/core/testing';

import { NgbActiveModal, NgbModal, NgbModalModule } from '@ng-bootstrap/ng-bootstrap';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { ModalService } from './modal.service';

@Component({
  template: ``
})
class MockComponent {
  foo = '';

  constructor(public activeModal: NgbActiveModal) {}
}

describe('ModalService', () => {
  let service: ModalService;
  let ngbModal: NgbModal;

  configureTestBed({ declarations: [MockComponent], imports: [NgbModalModule] }, [MockComponent]);

  beforeEach(() => {
    service = TestBed.inject(ModalService);
    ngbModal = TestBed.inject(NgbModal);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call NgbModal.open when show is called', () => {
    spyOn(ngbModal, 'open').and.callThrough();

    const modaRef = service.show(MockComponent, { foo: 'bar' });

    expect(ngbModal.open).toBeCalled();
    expect(modaRef.componentInstance.foo).toBe('bar');
    expect(modaRef.componentInstance.activeModal).toBeTruthy();
  });

  it('should call dismissAll and hasOpenModals', fakeAsync(() => {
    spyOn(ngbModal, 'dismissAll').and.callThrough();
    spyOn(ngbModal, 'hasOpenModals').and.callThrough();

    expect(ngbModal.hasOpenModals()).toBeFalsy();

    service.show(MockComponent, { foo: 'bar' });
    expect(service.hasOpenModals()).toBeTruthy();

    service.dismissAll();
    tick();
    expect(service.hasOpenModals()).toBeFalsy();

    expect(ngbModal.dismissAll).toBeCalled();
    expect(ngbModal.hasOpenModals).toBeCalled();
  }));
});
