import { Injectable } from '@angular/core';
import { FormlyJsonschema } from '@ngx-formly/core/json-schema';
import { JsonFormUISchema } from '../forms/crud-form/crud-form.model';

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
          controlSchema[i].className = uiSchema[j].className;
          controlSchema[i].props.templateOptions = uiSchema[j].templateOptions;
        }
      }
    }
    return { title, uiSchema, controlSchema };
  }
}
