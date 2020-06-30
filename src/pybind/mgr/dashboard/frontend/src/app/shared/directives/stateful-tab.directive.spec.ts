import { NgbNav, NgbNavChangeEvent, NgbNavConfig } from '@ng-bootstrap/ng-bootstrap';

import { StatefulTabDirective } from './stateful-tab.directive';

describe('StatefulTabDirective', () => {
  it('should create an instance', () => {
    const directive = new StatefulTabDirective(null);
    expect(directive).toBeTruthy();
  });

  it('should get and select active tab', () => {
    const nav = new NgbNav('tablist', new NgbNavConfig(), <any>null, null);
    spyOn(nav, 'select');
    spyOn(window.localStorage, 'getItem').and.returnValue('foo');
    const directive = new StatefulTabDirective(nav);
    directive.ngOnInit();
    expect(window.localStorage.getItem).toHaveBeenCalled();
    expect(nav.select).toHaveBeenCalledWith('foo');
  });

  it('should store active tab', () => {
    spyOn(window.localStorage, 'setItem');
    const directive = new StatefulTabDirective(null);
    directive.cdStatefulTab = 'bar';
    const event: NgbNavChangeEvent<string> = { activeId: '', nextId: 'xyz', preventDefault: null };
    directive.onNavChange(event);
    expect(window.localStorage.setItem).toHaveBeenCalledWith('tabset_bar', 'xyz');
  });
});
