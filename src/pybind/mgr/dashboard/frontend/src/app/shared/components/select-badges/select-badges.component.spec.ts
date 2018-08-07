import { ComponentFixture, TestBed } from '@angular/core/testing';

import { PopoverModule } from 'ngx-bootstrap';

import { FormsModule, ReactiveFormsModule, Validators } from '@angular/forms';
import { configureTestBed } from '../../../../testing/unit-test-helper';
import { SelectBadgesOption } from './select-badges-option.model';
import { SelectBadgesComponent } from './select-badges.component';

describe('SelectBadgesComponent', () => {
  let component: SelectBadgesComponent;
  let fixture: ComponentFixture<SelectBadgesComponent>;

  configureTestBed({
    declarations: [SelectBadgesComponent],
    imports: [PopoverModule.forRoot(), FormsModule, ReactiveFormsModule]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(SelectBadgesComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
    component.options = [
      { name: 'option1', description: '', selected: false },
      { name: 'option2', description: '', selected: false },
      { name: 'option3', description: '', selected: false }
    ];
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should add item', () => {
    component.data = [];
    component.triggerSelection(component.options[1]);
    expect(component.data).toEqual(['option2']);
  });

  it('should update selected', () => {
    component.data = ['option2'];
    component.ngOnChanges();
    expect(component.options[0].selected).toBe(false);
    expect(component.options[1].selected).toBe(true);
  });

  it('should remove item', () => {
    component.options.map((option) => {
      option.selected = true;
      return option;
    });
    component.data = ['option1', 'option2', 'option3'];
    component.removeItem('option1');
    expect(component.data).toEqual(['option2', 'option3']);
  });

  it('should not remove item that is not selected', () => {
    component.options[0].selected = true;
    component.data = ['option1'];
    component.removeItem('option2');
    expect(component.data).toEqual(['option1']);
  });

  describe('automatically add selected options if not in options array', () => {
    beforeEach(() => {
      component.data = ['option1', 'option4'];
      expect(component.options.length).toBe(3);
    });

    const expectedResult = () => {
      expect(component.options.length).toBe(4);
      expect(component.options[3]).toEqual(new SelectBadgesOption(true, 'option4', ''));
    };

    it('with no extra settings', () => {
      component.ngOnInit();
      expectedResult();
    });

    it('with custom badges', () => {
      component.customBadges = true;
      component.ngOnInit();
      expectedResult();
    });

    it('with limit higher than selected', () => {
      component.selectionLimit = 3;
      component.ngOnInit();
      expectedResult();
    });

    it('with limit equal to selected', () => {
      component.selectionLimit = 2;
      component.ngOnInit();
      expectedResult();
    });

    it('with limit lower than selected', () => {
      component.selectionLimit = 1;
      component.ngOnInit();
      expectedResult();
    });
  });

  describe('with custom options', () => {
    beforeEach(() => {
      component.customBadges = true;
      component.customBadgeValidators = [Validators.pattern('[A-Za-z0-9_]+')];
      component.ngOnInit();
      component.customBadge.setValue('customOption');
      component.addCustomOption();
    });

    it('adds custom option', () => {
      expect(component.options[3]).toEqual({
        name: 'customOption',
        description: '',
        selected: true
      });
      expect(component.data).toEqual(['customOption']);
    });

    it('will not add an option that did not pass the validation', () => {
      component.customBadge.setValue(' this does not pass ');
      component.addCustomOption();
      expect(component.options.length).toBe(4);
      expect(component.data).toEqual(['customOption']);
      expect(component.customBadge.invalid).toBeTruthy();
    });

    it('removes custom item selection by name', () => {
      component.removeItem('customOption');
      expect(component.data).toEqual([]);
      expect(component.options[3]).toEqual({
        name: 'customOption',
        description: '',
        selected: false
      });
    });

    it('will not add an option that is already there', () => {
      component.customBadge.setValue('option2');
      component.addCustomOption();
      expect(component.options.length).toBe(4);
      expect(component.data).toEqual(['customOption']);
    });

    it('will not add an option twice after each other', () => {
      component.customBadge.setValue('onlyOnce');
      component.addCustomOption();
      component.addCustomOption();
      expect(component.data).toEqual(['customOption', 'onlyOnce']);
      expect(component.options.length).toBe(5);
    });
  });

  describe('if the selection limit is reached', function() {
    beforeEach(() => {
      component.selectionLimit = 2;
      component.triggerSelection(component.options[0]);
      component.triggerSelection(component.options[1]);
    });

    it('will not select more options', () => {
      component.triggerSelection(component.options[2]);
      expect(component.data).toEqual(['option1', 'option2']);
    });

    it('will unselect options that are selected', () => {
      component.triggerSelection(component.options[1]);
      expect(component.data).toEqual(['option1']);
    });
  });
});
