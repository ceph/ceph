import { RbdConfigurationEntry } from '~/app/shared/models/configuration';

export class RbdFormEditRequestModel {
  name: string;
  size: number;
  features: Array<string> = [];
  configuration: RbdConfigurationEntry[];

  enable_mirror?: boolean;
  mirror_mode?: string;
  primary?: boolean;
  force?: boolean;
  schedule_interval: string;
  remove_scheduling? = false;
  image_mirror_mode?: string;
}
