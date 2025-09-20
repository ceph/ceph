import { SelectOption } from '~/app/shared/components/select/select-option.model';

export class UserFormRoleModel implements SelectOption {
  name: string;
  description: string;
  selected = false;
  scopes_permissions?: object;
  enabled: boolean;
  content: string;
  constructor(name: string, description: string, content: string) {
    this.name = name;
    this.description = description;
    this.content = content;
  }
}
