import { ComboBoxService } from '../services/combo-box.service';
import { DynamicInputComboboxDirective } from './dynamic-input-combobox.directive';

describe('DynamicInputComboboxDirective', () => {
  let comboBoxService: ComboBoxService;
  it('should create an instance', () => {
    const directive = new DynamicInputComboboxDirective(comboBoxService);
    expect(directive).toBeTruthy();
  });
});
