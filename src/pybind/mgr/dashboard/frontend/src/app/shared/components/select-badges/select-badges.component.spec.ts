import { ComponentFixture, TestBed } from '@angular/core/testing';

import { PopoverModule } from 'ngx-bootstrap';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { SelectBadgesComponent } from './select-badges.component';

describe('SelectBadgesComponent', () => {
  let component: SelectBadgesComponent;
  let fixture: ComponentFixture<SelectBadgesComponent>;

  configureTestBed({
    declarations: [SelectBadgesComponent],
    imports: [PopoverModule.forRoot()]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(SelectBadgesComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should add item', () => {
    component.options = [
      {name: 'option1', description: '', selected: false},
      {name: 'option2', description: '', selected: false}
    ];
    component.data = [];
    component.selectOption(component.options[1]);
    expect(component.data).toEqual(['option2']);
  });

  it('should update selected', () => {
    component.options = [
      {name: 'option1', description: '', selected: false},
      {name: 'option2', description: '', selected: false}
    ];
    component.data = ['option2'];
    component.ngOnChanges();
    expect(component.options[0].selected).toBe(false);
    expect(component.options[1].selected).toBe(true);
  });

  it('should remove item', () => {
    component.options = [
      {name: 'option1', description: '', selected: true},
      {name: 'option2', description: '', selected: true}
    ];
    component.data = ['option1', 'option2'];
    component.removeItem('option1');
    expect(component.data).toEqual(['option2']);
  });

});
