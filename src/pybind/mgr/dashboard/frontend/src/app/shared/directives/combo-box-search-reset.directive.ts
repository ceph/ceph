import { Directive, HostListener, inject } from '@angular/core';

import { ComboBox } from 'carbon-components-angular';

import { ComboBoxType } from '../enum/combo-box-type.enum';

/**
 * Carbon multi combo-boxes keep typed filter text in the search input after an
 * item is selected. Apply `cdComboBoxSearchReset` on `cds-combo-box` to reset
 * that leftover search state without closing the dropdown, so additional items
 * can still be selected.
 */
@Directive({
  selector: '[cdComboBoxSearchReset]',
  standalone: false
})
export class ComboBoxSearchResetDirective {
  private comboBox = inject(ComboBox, { optional: true });

  @HostListener('selected')
  onSelected(): void {
    if (this.comboBox?.type !== ComboBoxType.Multi) {
      return;
    }
    // Let Carbon finish updating pills before resetting the filter input.
    Promise.resolve().then(() => this.resetSearchInput());
  }

  private resetSearchInput(): void {
    if (!this.comboBox) {
      return;
    }

    const inputEl = this.comboBox.input?.nativeElement as HTMLInputElement | undefined;

    if (!inputEl?.value) {
      return;
    }

    inputEl.value = '';
    this.comboBox.selectedValue = '';
    this.comboBox.showClearButton = false;
    this.comboBox.onSearch('', false);
  }
}
