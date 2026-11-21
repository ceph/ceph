import { Component, Input, OnChanges, SimpleChanges } from '@angular/core';

import { ControlValueAccessor, NG_VALUE_ACCESSOR, ReactiveFormsModule } from '@angular/forms';
import { ButtonModule, GridModule, IconModule, InputModule } from 'carbon-components-angular';
import { ComponentsModule } from '../components.module';

@Component({
  selector: 'cd-text-label-list',
  imports: [
    ReactiveFormsModule,
    InputModule,
    IconModule,
    ButtonModule,
    GridModule,
    ComponentsModule
  ],
  templateUrl: './text-label-list.component.html',
  styleUrl: './text-label-list.component.scss',
  providers: [
    {
      provide: NG_VALUE_ACCESSOR,
      useExisting: TextLabelListComponent,
      multi: true
    }
  ]
})
export class TextLabelListComponent implements ControlValueAccessor, OnChanges {
  // Label for the text list.
  @Input() label: string = '';
  // Optional helper text that appears under the label.
  @Input() helperText: string = '';
  // Value displayed if no item is present.
  @Input() placeholder: string = '';
  // When set, show an explicit Add button and ghost trash on every row
  // instead of auto-appending an empty trailing input while typing.
  @Input() addButtonLabel: string = '';

  values: string[] = [''];
  disabled: boolean = false;
  touched: boolean = false;

  private onChange: (value: string[]) => void = () => {};
  private onTouched: () => void = () => {};

  private get useAddButton(): boolean {
    return !!this.addButtonLabel;
  }

  ngOnChanges(changes: SimpleChanges): void {
    // writeValue may run before addButtonLabel is bound; re-normalize when it arrives.
    if (changes['addButtonLabel'] && this.useAddButton) {
      this.values = this.normalizeValues(this.values.filter((v) => v.trim() !== ''));
    }
  }

  private _isRemovingChar(originalValue: string, value: string): boolean {
    return originalValue.slice(0, -1) === value;
  }

  inputId(index: number): string {
    return `cd-text-label-list-${index}`;
  }

  private normalizeValues(items: string[]): string[] {
    if (this.useAddButton) {
      return items.length ? items : [''];
    }
    return items.length ? [...items, ''] : [''];
  }

  writeValue(value: string[] | string | null): void {
    let items: string[] = [];
    if (typeof value === 'string') {
      items = value ? [value] : [];
    } else if (Array.isArray(value)) {
      items = [...value];
    }
    this.values = this.normalizeValues(items);
  }

  registerOnChange(onChange: (value: string[]) => void): void {
    this.onChange = onChange;
  }

  registerOnTouched(onTouched: () => void): void {
    this.onTouched = onTouched;
  }

  setDisabledState(disabled: boolean): void {
    this.disabled = disabled;
  }

  onInputChange(index: number, value: string) {
    const originalValue = this.values[index];
    this.values[index] = value;

    if (
      !this.useAddButton &&
      !this._isRemovingChar(originalValue, value) &&
      index === this.values.length - 1 &&
      value.trim() !== ''
    ) {
      this.values.push('');
    }

    this.emitChange();
    this.markAsTouched();
  }

  addInput() {
    this.values.push('');
    this.markAsTouched();
  }

  markAsTouched() {
    if (!this.touched) {
      this.onTouched();
      this.touched = true;
    }
  }

  deleteInput(index: number) {
    if (this.values.length === 1) {
      this.values[0] = '';
    } else {
      this.values.splice(index, 1);
    }

    if (this.values.length === 0) {
      this.values.push('');
    }

    this.emitChange();
    this.markAsTouched();
  }

  private emitChange() {
    const nonEmpty = this.values.filter((v) => v.trim() !== '');
    this.onChange(nonEmpty.length ? nonEmpty : []);
  }
}
