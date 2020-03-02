import { byText, createTestComponentFactory, Spectator } from '@ngneat/spectator';
import { MockComponent, MockModule } from 'ng-mocks';

import { CollapseModule } from 'ngx-bootstrap/collapse';
import { SimplebarAngularModule } from 'simplebar-angular';

import { FeatureDirective } from '../../../shared/directives/feature.directive';
import { MenuItemComponent } from '../menu-item/menu-item.component';
import { MenuParentItemComponent } from '../menu-parent-item/menu-parent-item.component';
import { MenuComponent } from './menu.component';

describe('MenuComponent', () => {
  let spectator: Spectator<MenuComponent>;

  const createComponent = createTestComponentFactory({
    component: MenuComponent,
    declarations: [MenuParentItemComponent, MockComponent(MenuItemComponent), FeatureDirective],
    imports: [MockModule(SimplebarAngularModule), MockModule(CollapseModule)]
  });

  beforeEach(() => {
    spectator = createComponent();
    spectator.setInput('items', [
      {
        label: 'Parent Menu 1',
        children: [
          { label: 'Submenu 1-1', link: '/submenu11' },
          { label: 'Submenu 1-2', link: '/submenu12' },
          { label: 'Submenu 1-3', link: '/submenu13' }
        ]
      },
      {
        label: 'Parent Menu 2',
        children: [
          { label: 'Submenu 2-1', link: '/submenu21' },
          { label: 'Submenu 2-2', link: '/submenu22' },
          { label: 'Submenu 2-3', link: '/submenu23' }
        ]
      },
      { label: 'Parent Menu 3', link: '/menu3' }
    ]);
  });

  it('displays menu hierarchy', () => {
    expect('menu-parent-item').toHaveLength(2);
    expect('menu-parent-item menu-item').toHaveLength(6);
    expect('menu-item').toHaveLength(7);
  });

  it('everything collapsed', () => {
    expect(spectator.queryAll(MenuParentItemComponent).map((i) => i.expanded)).toEqual([
      false,
      false
    ]);
  });

  it('on click expands and collapses parent items', () => {
    // Click Parent 1: it expands
    spectator.click(spectator.query(byText('Parent Menu 1')));
    expect(spectator.component.childExpanded).toBe(spectator.component.items[0]);
    expect(spectator.queryAll(MenuParentItemComponent).map((i) => i.expanded)).toEqual([
      true,
      false
    ]);

    // Click Parent 2: it expands and Parent 1 collapses
    spectator.click(spectator.query(byText('Parent Menu 2')));
    expect(spectator.component.childExpanded).toBe(spectator.component.items[1]);
    expect(spectator.queryAll(MenuParentItemComponent).map((i) => i.expanded)).toEqual([
      false,
      true
    ]);

    // Click Parent 2 again: everything collapsed
    spectator.click(spectator.query(byText('Parent Menu 2')));
    expect(spectator.component.childExpanded).toBeUndefined();
    expect(spectator.queryAll(MenuParentItemComponent).map((i) => i.expanded)).toEqual([
      false,
      false
    ]);
  });
});
