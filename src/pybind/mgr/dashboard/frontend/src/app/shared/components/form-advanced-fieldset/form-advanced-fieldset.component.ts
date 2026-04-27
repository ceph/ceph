import { Component, Input } from '@angular/core';

@Component({
  selector: 'cd-form-advanced-fieldset',
  templateUrl: './form-advanced-fieldset.component.html',
  styleUrls: ['./form-advanced-fieldset.component.scss'],
  standalone: false
})
export class FormAdvancedFieldsetComponent {
  @Input()
  title: string = 'Advanced';
  showAdvanced: boolean = false;
}
