import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { from } from 'rxjs';
import { map, pluck } from 'rxjs/operators';
import {
  configureTestBed,
  FixtureHelper,
  i18nProviders
} from '../../../../testing/unit-test-helper';
import { BadgeComponent } from './badge.component';

describe('BadgeComponent', () => {
  let component: BadgeComponent;
  let fixture: ComponentFixture<BadgeComponent>;
  let fixtureHelper: FixtureHelper;

  configureTestBed({
    imports: [RouterTestingModule],
    declarations: [BadgeComponent],
    providers: [i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(BadgeComponent);
    fixtureHelper = new FixtureHelper(fixture);
    component = fixture.componentInstance;
    component.config = {
      onChange: from([{ health_status: 'HEALTH_OK' }]).pipe(
        pluck('health_status'),
        map((s) => ({ class: 'test_class', label: s, style: { color: 'blue' }, hidden: false }))
      )
    };
  });

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
    fixtureHelper.expectElementVisible('span', true);
    const element = fixtureHelper.getElementByCss('span');
    fixtureHelper.expectTextToBe('span', 'HEALTH_OK');
    expect(element.classes).toEqual({ test_class: true });
    expect(element.styles).toEqual({ color: 'blue' });
    expect(element.properties).toEqual({ hidden: false });
  });
});
