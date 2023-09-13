import { NgbConfig, NgbNav, NgbNavChangeEvent, NgbNavConfig } from '@ng-bootstrap/ng-bootstrap';

import { StatefulTabDirective } from './stateful-tab.directive';

describe('StatefulTabDirective', () => {
  it('should create an instance', () => {
    const directive = new StatefulTabDirective(null);
    expect(directive).toBeTruthy();
  });

  it('should get and select active tab', () => {
    const nav = new NgbNav('tablist', new NgbNavConfig(new NgbConfig()), <any>null, null);
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
    const nav = new NgbNav('tablist', new NgbNavConfig(new NgbConfig()), <any>null, null);
    spyOn(nav, 'select');
    const directive = new StatefulTabDirective(nav);
    directive.cdStatefulTab = 'bar';
    directive.cdStatefulTabDefault = 'defaultTab';
    directive.ngOnInit();
    expect(nav.select).toHaveBeenCalledWith('defaultTab');
  });
});
