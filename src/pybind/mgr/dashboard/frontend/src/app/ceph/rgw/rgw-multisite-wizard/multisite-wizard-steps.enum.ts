export enum StepTitles {
  CreateRealmAndZonegroup = 'Create realm & zonegroup',
  CreateZone = 'Create zone',
  SelectCluster = 'Select cluster',
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

export const STEP_TITLES_EXISTING_REALM = [
  StepTitles.CreateRealmAndZonegroup,
  StepTitles.SelectCluster,
  StepTitles.Review
];
