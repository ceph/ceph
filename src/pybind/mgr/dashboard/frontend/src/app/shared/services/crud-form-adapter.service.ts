import { Injectable } from '@angular/core';
import { FormlyJsonschema } from '@ngx-formly/core/json-schema';
import { CrudTaskInfo, JsonFormUISchema } from '../forms/crud-form/crud-form.model';
import { setupValidators } from '../forms/crud-form/helpers';

@Injectable({
  providedIn: 'root'
})
export class CrudFormAdapterService {
  constructor(private formlyJsonschema: FormlyJsonschema) {}

  processJsonSchemaForm(response: any): JsonFormUISchema {
    const title = response.forms[0].control_schema.title;
    const uiSchema = response.forms[0].ui_schema;
    const cSchema = response.forms[0].control_schema;
    let controlSchema = this.formlyJsonschema.toFieldConfig(cSchema).fieldGroup;
    for (let i = 0; i < controlSchema.length; i++) {
      for (let j = 0; j < uiSchema.length; j++) {
        if (controlSchema[i].key == uiSchema[j].key) {
          controlSchema[i].props.templateOptions = uiSchema[j].templateOptions;
          setupValidators(controlSchema[i], uiSchema);
        }
      }
    }
    let taskInfo: CrudTaskInfo = {
      metadataFields: response.forms[0].task_info.metadataFields,
      message: response.forms[0].task_info.message
    };
    return { title, uiSchema, controlSchema, taskInfo };
  }
}
