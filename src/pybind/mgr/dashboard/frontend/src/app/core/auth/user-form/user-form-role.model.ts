import { SelectBadgesOption } from '../../../shared/components/select-badges/select-badges-option.model';

export class UserFormRoleModel implements SelectBadgesOption {
  name: string;
  description: string;
  selected = false;
  scopes_permissions: object;

  constructor(name, description) {
    this.name = name;
    this.description = description;
  }
}
