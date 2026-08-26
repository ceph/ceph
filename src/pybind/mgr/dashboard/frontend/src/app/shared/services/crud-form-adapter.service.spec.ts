import { TestBed } from '@angular/core/testing';

import { CrudFormAdapterService } from './crud-form-adapter.service';
import { RouterTestingModule } from '@angular/router/testing';
import { FormlyModule } from '@ngx-formly/core';

describe('CrudFormAdapterService', () => {
  let service: CrudFormAdapterService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [RouterTestingModule, FormlyModule.forRoot()]
    });
    service = TestBed.inject(CrudFormAdapterService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should map key type enum options from the create user schema', () => {
    const response = {
      forms: [
        {
          path: '/cluster/user/create',
          method_type: 'post',
          task_info: { metadataFields: ['user_entity'], message: 'created' },
          control_schema: {
            type: 'object',
            title: 'Create User',
            properties: {
              user_entity: { type: 'string', title: 'User entity' },
              key_type: {
                type: 'string',
                title: 'Key type',
                default: 'aes',
                enum: ['aes', 'aes256k']
              },
              capabilities: {
                type: 'array',
                title: 'Capabilities',
                minItems: 1,
                items: {
                  type: 'object',
                  properties: {
                    entity: { type: 'string', title: 'Entity' },
                    cap: { type: 'string', title: 'Entity Capabilities' }
                  },
                  required: ['entity', 'cap']
                }
              }
            },
            required: ['user_entity', 'key_type', 'capabilities']
          },
          ui_schema: [
            { templateOptions: { layoutType: 'column' }, key: '', items: [] },
            { key: 'user_entity', readonly: false, help: '', validators: [] },
            { key: 'key_type', readonly: false, help: '', validators: [] },
            { key: 'capabilities', templateOptions: {}, items: [] }
          ]
        }
      ]
    };

    const result = service.processJsonSchemaForm(response, '/cluster/user/create');
    const keyTypeField = result.controlSchema.find((field) => field.key === 'key_type');

    expect(result.title).toBe('Create User');
    expect(keyTypeField).toBeTruthy();
    expect(keyTypeField.type).toBe('enum');
    expect(keyTypeField.defaultValue).toBe('aes');
    expect(keyTypeField.props.options).toEqual([
      { label: 'aes', value: 'aes' },
      { label: 'aes256k', value: 'aes256k' }
    ]);
  });
});
