import { FormlyFieldConfig } from '@ngx-formly/core';

export interface JsonFormUISchema {
  title: string;
  controlSchema: FormlyFieldConfig[];
  uiSchema: any;
}
