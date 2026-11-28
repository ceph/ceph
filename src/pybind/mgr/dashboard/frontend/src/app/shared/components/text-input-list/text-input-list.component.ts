import { Component, Input } from '@angular/core';

import { ControlValueAccessor, NG_VALUE_ACCESSOR, ReactiveFormsModule } from '@angular/forms';
import { ButtonModule, GridModule, IconModule, InputModule } from 'carbon-components-angular';

import { ComponentsModule } from '../components.module';

/**
 * Editable string list with an explicit Add button and per-row remove.
 * Prefer this over cd-text-label-list when values should not auto-append
 * an empty trailing input while typing.
 *
 * Validation stays with the parent form (e.g. CdValidators); pass invalid /
 * invalidText to surface errors in this control.
 */
@Component({
  selector: 'cd-text-input-list',
  imports: [
    ReactiveFormsModule,
    InputModule,
    IconModule,
    ButtonModule,
    GridModule,
    ComponentsModule
  ],
  templateUrl: './text-input-list.component.html',
  styleUrl: './text-input-list.component.scss',
  providers: [
    {
      provide: NG_VALUE_ACCESSOR,
      useExisting: TextInputListComponent,
      multi: true
    }
  ]
})
export class TextInputListComponent implements ControlValueAccessor {
  private static nextInstanceId = 0;

  @Input() label: string = '';
  @Input() helperText: string = '';
  @Input() placeholder: string = '';
  @Input() addButtonLabel: string = $localize`Add`;
  /**
   * Marks the list as invalid (e.g. from parent form control state).
   */
  @Input() invalid: boolean = false;
  /**
   * Error message shown when the list is invalid. Provided by the caller
   * so this shared component stays reusable.
   */
  @Input() invalidText: string = '';

  values: string[] = [''];
  disabled: boolean = false;
  touched: boolean = false;

  private readonly instanceId = TextInputListComponent.nextInstanceId++;
  private onChange: (value: string[]) => void = () => {};
  private onTouched: () => void = () => {};

  inputId(index: number): string {
    return `cd-text-input-list-${this.instanceId}-${index}`;
  }

  get showInvalid(): boolean {
    return this.invalid && !!this.invalidText;
  }

  writeValue(value: string[] | string | null): void {
    let items: string[] = [];
    if (typeof value === 'string') {
      items = value ? [value] : [];
    } else if (Array.isArray(value)) {
      items = [...value];
    }
    this.values = items.length ? items : [''];
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
    this.values[index] = value;
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
