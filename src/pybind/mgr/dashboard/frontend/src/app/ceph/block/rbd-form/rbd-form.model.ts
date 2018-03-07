export class RbdFormModel {
  name: string;
  useDataPool: boolean;
  pool: string;
  dataPool: string;
  size: number;
  obj_size: number;
  defaultFeatures: boolean;
  striping = {
    count: 5,
    unit: 4194304,
    unitDisplayed: '4 MiB'
  };
  features = {
    'deep-flatten': {
      checked: false,
      disabled: false,
      canUpdate: false
    },
    'layering': {
      checked: false,
      disabled: false,
      canUpdate: false
    },
    'striping': {
      checked: false,
      disabled: false,
      canUpdate: false
    },
    'exclusive-lock': {
      checked: false,
      disabled: false,
      canUpdate: true
    },
    'object-map': {
      checked: false,
      disabled: true,
      canUpdate: true
    },
    'journaling': {
      checked: false,
      disabled: true,
      canUpdate: true
    },
    'fast-diff': {
      checked: false,
      disabled: true,
      canUpdate: true
    }
  };
}
