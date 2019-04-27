import { RbdConfigurationEntry } from '../../../shared/models/configuration';

export class RbdFormEditRequestModel {
  name: string;
  size: number;
  features: Array<string> = [];
  configuration: RbdConfigurationEntry[];
}
