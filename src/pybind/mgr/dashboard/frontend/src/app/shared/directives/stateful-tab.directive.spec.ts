import { NgbNav, NgbNavChangeEvent } from '@ng-bootstrap/ng-bootstrap';
import { CUSTOM_ELEMENTS_SCHEMA, NO_ERRORS_SCHEMA } from '@angular/core';

import { StatefulTabDirective } from './stateful-tab.directive';
import { TestBed } from '@angular/core/testing';

class NgbNavMock {
  select() {}
}

describe('StatefulTabDirective', () => {
  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [StatefulTabDirective],
      providers: [{ provide: NgbNav, useClass: NgbNavMock }],
      schemas: [NO_ERRORS_SCHEMA, CUSTOM_ELEMENTS_SCHEMA]
    }).compileComponents();
  });

  it('should create an instance', () => {
    const directive = new StatefulTabDirective(null);
    expect(directive).toBeTruthy();
  });

  it('should get and select active tab', () => {
    const nav = TestBed.inject(NgbNav);
    spyOn(nav, 'select');
    const directive = new StatefulTabDirective(nav);
    directive.cdStatefulTab = 'bar';
    window.localStorage.setItem('tabset_bar', 'foo');
    directive.ngOnInit();
    expect(nav.select).toHaveBeenCalledWith('foo');
  });

  it('should store active tab', () => {
    const directive = new StatefulTabDirective(null);
    directive.cdStatefulTab = 'bar';
    const event: NgbNavChangeEvent<string> = { activeId: '', nextId: 'xyz', preventDefault: null };
    directive.onNavChange(event);
    expect(window.localStorage.getItem('tabset_bar')).toBe('xyz');
  });

  it('should select the default tab if provided', () => {
    const nav = TestBed.inject(NgbNav);
    spyOn(nav, 'select');
    const directive = new StatefulTabDirective(nav);
    directive.cdStatefulTab = 'bar';
    directive.cdStatefulTabDefault = 'defaultTab';
    directive.ngOnInit();
    expect(nav.select).toHaveBeenCalledWith('defaultTab');
  });
});
