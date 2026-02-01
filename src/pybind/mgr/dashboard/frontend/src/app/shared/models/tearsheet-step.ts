import { TemplateRef } from '@angular/core';
import { FormGroup } from '@angular/forms';

export interface TearsheetStep {
  formGroup: FormGroup;
  rightInfluencer?: TemplateRef<any>;
  showRightInfluencer?: () => boolean;
}
