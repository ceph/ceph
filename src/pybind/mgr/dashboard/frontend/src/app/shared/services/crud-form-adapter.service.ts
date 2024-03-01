import { Injectable } from '@angular/core';
import { FormlyJsonschema } from '@ngx-formly/core/json-schema';
import { CrudTaskInfo, JsonFormUISchema } from '../forms/crud-form/crud-form.model';
import { setupValidators } from '../forms/crud-form/helpers';

@Injectable({
  providedIn: 'root'
})
export class CrudFormAdapterService {
  constructor(private formlyJsonschema: FormlyJsonschema) {}

  processJsonSchemaForm(response: any, path: string): JsonFormUISchema {
    let form = 0;
    while (form < response.forms.length) {
      if (response.forms[form].path == path) {
        break;
      }
      form++;
    }
    form %= response.forms.length;
    const title = response.forms[form].control_schema.title;
    const uiSchema = response.forms[form].ui_schema;
    const cSchema = response.forms[form].control_schema;
    let controlSchema = this.formlyJsonschema.toFieldConfig(cSchema).fieldGroup;
    for (let i = 0; i < controlSchema.length; i++) {
      for (let j = 0; j < uiSchema.length; j++) {
        if (controlSchema[i].key == uiSchema[j].key) {
          controlSchema[i].props.templateOptions = uiSchema[j].templateOptions;
          controlSchema[i].props.readonly = uiSchema[j].readonly;
          setupValidators(controlSchema[i], uiSchema);
        }
      }
    }
    let taskInfo: CrudTaskInfo = {
      metadataFields: response.forms[form].task_info.metadataFields,
      message: response.forms[form].task_info.message
    };
    const methodType = response.forms[form].method_type;
    const model = response.forms[form].model || {};
    return { title, uiSchema, controlSchema, taskInfo, methodType, model };
  }
}
