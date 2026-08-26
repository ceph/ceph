import { ConfigOption } from '@ngx-formly/core';
import { FormlyArrayTypeComponent } from './formly-array-type/formly-array-type.component';
import { FormlyFileTypeComponent } from './formly-file-type/formly-file-type.component';
import { FormlyInputTypeComponent } from './formly-input-type/formly-input-type.component';
import { FormlyInputWrapperComponent } from './formly-input-wrapper/formly-input-wrapper.component';
import { FormlyObjectTypeComponent } from './formly-object-type/formly-object-type.component';
import { FormlySelectTypeComponent } from './formly-select-type/formly-select-type.component';
import { FormlyTextareaTypeComponent } from './formly-textarea-type/formly-textarea-type.component';

export const FORMLY_CARBON_CONFIG: ConfigOption = {
  types: [
    { name: 'array', component: FormlyArrayTypeComponent, wrappers: [] },
    { name: 'object', component: FormlyObjectTypeComponent, wrappers: [] },
    { name: 'input', component: FormlyInputTypeComponent, wrappers: [] },
    { name: 'string', component: FormlyInputTypeComponent, wrappers: [] },
    { name: 'textarea', component: FormlyTextareaTypeComponent, wrappers: [] },
    { name: 'file', component: FormlyFileTypeComponent, wrappers: [] },
    {
      name: 'enum',
      component: FormlySelectTypeComponent,
      wrappers: [],
      defaultOptions: { props: { hideRequiredMarker: true } }
    },
    {
      name: 'select',
      component: FormlySelectTypeComponent,
      wrappers: [],
      defaultOptions: { props: { hideRequiredMarker: true } }
    }
  ],
  validationMessages: [
    { name: 'required', message: 'This field is required' },
    { name: 'json', message: 'This field is not a valid json document' },
    {
      name: 'rgwRoleName',
      message:
        'Role name must contain letters, numbers or the ' +
        'following valid special characters "_+=,.@-]+" (pattern: [0-9a-zA-Z_+=,.@-]+)'
    },
    {
      name: 'rgwRolePath',
      message:
        'Role path must start and finish with a slash "/".' +
        ' (pattern: (\u002F)|(\u002F[\u0021-\u007E]+\u002F))'
    },
    { name: 'file_size', message: 'File size must not exceed 4KiB' },
    {
      name: 'rgwRoleSessionDuration',
      message: 'This field must be a number and should be a value from 1 hour to 12 hour'
    }
  ],
  wrappers: [{ name: 'input-wrapper', component: FormlyInputWrapperComponent }]
};
