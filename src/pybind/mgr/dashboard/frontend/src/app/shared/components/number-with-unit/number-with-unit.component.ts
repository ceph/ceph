import { Component, Input, Optional, TemplateRef } from '@angular/core';
import { ControlContainer, FormGroup } from '@angular/forms';

@Component({
  selector: 'cd-number-with-unit',
  templateUrl: './number-with-unit.component.html',
  styleUrls: ['./number-with-unit.component.scss'],
  standalone: false
})
export class NumberWithUnitComponent {
  @Input()
  form: FormGroup;

  @Input()
  valueControlName: string;

  /** Resolved form: parent form from ControlContainer when inside a form, otherwise @Input() form. */
  get formGroup(): FormGroup {
    const parent = this.controlContainer?.control as FormGroup | undefined;
    return parent ?? this.form;
  }

  constructor(@Optional() private controlContainer: ControlContainer) {}

  @Input()
  unitControlName: string;

  @Input()
  label: string;

  @Input()
  helperText: string;

  @Input()
  units: string[] = ['KiB', 'MiB', 'GiB', 'TiB'];

  @Input()
  min: number = 0;

  @Input()
  max: number | undefined;

  @Input()
  unitLabel: string = $localize`Unit`;

  @Input()
  invalidText: string | TemplateRef<unknown> = $localize`The value should be greater or equal to 0`;

  get valueControl() {
    return this.formGroup?.get(this.valueControlName);
  }

  get unitControl() {
    return this.formGroup?.get(this.unitControlName);
  }

  get invalidTextIsTemplate(): boolean {
    return this.invalidText instanceof TemplateRef;
  }
}
