import { ComponentFixture, TestBed } from '@angular/core/testing';

import { TablePaginationComponent } from './table-pagination.component';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('TablePaginationComponent', () => {
  let component: TablePaginationComponent;
  let fixture: ComponentFixture<TablePaginationComponent>;
  let element: HTMLElement;

  configureTestBed({
    declarations: [TablePaginationComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(TablePaginationComponent);
    component = fixture.componentInstance;
    element = fixture.debugElement.nativeElement;
    component.page = 1;
    component.size = 10;
    component.count = 100;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should contain valid inputs', () => {
    expect(component.page).toEqual(1);
    expect(component.size).toEqual(10);
    expect(component.count).toEqual(100);
  });

  it('should change page', () => {
    const input = element.querySelector('input');
    input.value = '5';
    input.dispatchEvent(new Event('input'));
    expect(component.page).toEqual(5);
  });

  it('should disable prev button', () => {
    const prev: HTMLButtonElement = element.querySelector('.pagination__btn_prev');
    expect(prev.disabled).toBeTruthy();
  });

  it('should disable next button', () => {
    const next: HTMLButtonElement = element.querySelector('.pagination__btn_next');
    component.size = 100;
    fixture.detectChanges();
    expect(next.disabled).toBeTruthy();
  });
});
