import { RbdConfigurationEntry } from '~/app/shared/models/configuration';

export class RbdFormEditRequestModel {
  name: string;
  size: number;
  features: Array<string> = [];
  configuration: RbdConfigurationEntry[];
}
