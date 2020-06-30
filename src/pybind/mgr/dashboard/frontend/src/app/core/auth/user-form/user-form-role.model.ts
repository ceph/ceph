import { SelectOption } from '../../../shared/components/select/select-option.model';

export class UserFormRoleModel implements SelectOption {
  name: string;
  description: string;
  selected = false;
  scopes_permissions: object;
  enabled = true;

  constructor(name: string, description: string) {
    this.name = name;
    this.description = description;
  }
}
