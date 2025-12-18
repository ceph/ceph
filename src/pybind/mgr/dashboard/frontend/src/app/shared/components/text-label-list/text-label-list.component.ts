import { Component, Input } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ControlValueAccessor, NG_VALUE_ACCESSOR, ReactiveFormsModule } from '@angular/forms';
import { ButtonModule, GridModule, IconModule, InputModule } from 'carbon-components-angular';
import { ComponentsModule } from '../components.module';

@Component({
  selector: 'cd-text-label-list',
  standalone: true,
  imports: [
    CommonModule,
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
export class TextLabelListComponent implements ControlValueAccessor {
  // Label for the text list.
  @Input() label: string = '';
  // Optional helper text that appears under the label.
  @Input() helperText: string = '';
  // Value displayed if no item is present.
  @Input() placeholder: string = '';

  values: string[] = [''];
  disabled: boolean = false;
  touched: boolean = false;

  private onChange: (value: string[]) => void = () => {};
  private onTouched: () => void = () => {};

  private _isRemovingChar(originalValue: string, value: string): boolean {
    return originalValue.slice(0, -1) === value;
  }

  writeValue(value: string[]): void {
    this.values = value?.length ? [...value, ''] : [''];
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
      !this._isRemovingChar(originalValue, value) &&
      index === this.values.length - 1 &&
      value.trim() !== ''
    ) {
      this.values.push('');
    }

    const nonEmptyValues = this.values.filter((v) => v.trim() !== '');
    this.onChange(nonEmptyValues.length ? nonEmptyValues : []);

    this.markAsTouched();
  }

  markAsTouched() {
    if (!this.touched) {
      this.onTouched();
      this.touched = true;
    }
  }

  deleteInput(index: number) {
    this.values.splice(index, 1);

    if (this.values.length === 0) {
      this.values.push('');
    }

    const nonEmpty = this.values.filter((v) => v.trim() !== '');
    this.onChange(nonEmpty.length ? nonEmpty : []);
  }
}
