import { FormlyFieldConfig } from '@ngx-formly/core';

export interface CrudTaskInfo {
  metadataFields: string[];
  message: string;
}

export interface JsonFormUISchema {
  title: string;
  controlSchema: FormlyFieldConfig[];
  uiSchema: any;
  taskInfo: CrudTaskInfo;
  methodType: string;
  model: any;
}
