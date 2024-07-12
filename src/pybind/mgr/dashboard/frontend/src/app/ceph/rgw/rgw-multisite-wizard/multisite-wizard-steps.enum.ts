export enum StepTitles {
  CreateRealmAndZonegroup = 'Create Realm & Zonegroup',
  CreateZone = 'Create Zone',
  SelectCluster = 'Select Cluster',
  Review = 'Review'
}

export const STEP_TITLES_MULTI_CLUSTER_CONFIGURED = [
  StepTitles.CreateRealmAndZonegroup,
  StepTitles.CreateZone,
  StepTitles.SelectCluster,
  StepTitles.Review
];

export const STEP_TITLES_SINGLE_CLUSTER = [
  StepTitles.CreateRealmAndZonegroup,
  StepTitles.CreateZone,
  StepTitles.Review
];
