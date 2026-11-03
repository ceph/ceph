import { fakeAsync, TestBed, tick } from '@angular/core/testing';

import { ComboBox } from 'carbon-components-angular';

import { ComboBoxType } from '../enum/combo-box-type.enum';
import { ComboBoxSearchResetDirective } from './combo-box-search-reset.directive';

describe('ComboBoxSearchResetDirective', () => {
  const createDirective = (type: ComboBox['type'], inputValue = 'sm') => {
    const input = { value: inputValue };
    const comboBox = {
      type,
      input: { nativeElement: input },
      showClearButton: !!inputValue,
      selectedValue: '',
      onSearch: jasmine.createSpy('onSearch'),
      closeDropdown: jasmine.createSpy('closeDropdown')
    } as any;

    TestBed.configureTestingModule({
      providers: [ComboBoxSearchResetDirective, { provide: ComboBox, useValue: comboBox }]
    });

    const directive = TestBed.inject(ComboBoxSearchResetDirective);
    return { directive, comboBox, input };
  };

  it('should create an instance', () => {
    const { directive } = createDirective(ComboBoxType.Multi);
    expect(directive).toBeTruthy();
  });

  it('should reset search text after a multi combo-box item is selected', fakeAsync(() => {
    const { directive, comboBox, input } = createDirective(ComboBoxType.Multi);

    directive.onSelected();
    tick();

    expect(input.value).toBe('');
    expect(comboBox.selectedValue).toBe('');
    expect(comboBox.showClearButton).toBeFalsy();
    expect(comboBox.onSearch).toHaveBeenCalledWith('', false);
    expect(comboBox.closeDropdown).not.toHaveBeenCalled();
  }));

  it('should not reset search text for a single combo-box', fakeAsync(() => {
    const { directive, comboBox, input } = createDirective(ComboBoxType.Single);

    directive.onSelected();
    tick();

    expect(input.value).toBe('sm');
    expect(comboBox.onSearch).not.toHaveBeenCalled();
    expect(comboBox.closeDropdown).not.toHaveBeenCalled();
  }));

  it('should not reset Carbon state when the search input is already empty', fakeAsync(() => {
    const { directive, comboBox } = createDirective(ComboBoxType.Multi, '');

    directive.onSelected();
    tick();

    expect(comboBox.onSearch).not.toHaveBeenCalled();
  }));
});
