module System = { Scenario, Driver, Store, Rgw };

// roles: DeleteRole racing a policy write
machine TcDelRoleVsPutMain { start state S { entry { var c: tCfg; c = Main();  new Scenario((sc = SC_DEL_ROLE_VS_PUT, cfg = c)); } } }
test tcDelRoleVsPutMain [main=TcDelRoleVsPutMain]:
  assert DeleteNeedsEmpty in (union System, { TcDelRoleVsPutMain });
machine TcDelRoleVsPutAns { start state S { entry { var c: tCfg; c = Main();  new Scenario((sc = SC_DEL_ROLE_VS_PUT, cfg = c)); } } }
test tcDelRoleVsPutAns [main=TcDelRoleVsPutAns]:
  assert IamAnswers in (union System, { TcDelRoleVsPutAns });
machine TcDelRoleVsPutById { start state S { entry { var c: tCfg; c = Main(); c.roleDeleteById = true; new Scenario((sc = SC_DEL_ROLE_VS_PUT, cfg = c)); } } }
test tcDelRoleVsPutById [main=TcDelRoleVsPutById]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, NoLostUpdate in (union System, { TcDelRoleVsPutById });
machine TcDelRoleVsPutNeedsRole { start state S { entry { var c: tCfg; c = Main(); c.roleDeleteById = true; c.roleWriteNeedsRole = true; new Scenario((sc = SC_DEL_ROLE_VS_PUT, cfg = c)); } } }
test tcDelRoleVsPutNeedsRole [main=TcDelRoleVsPutNeedsRole]:
  assert IamAnswers in (union System, { TcDelRoleVsPutNeedsRole });
machine TcDelRoleVsAttachMain { start state S { entry { var c: tCfg; c = Main();  new Scenario((sc = SC_DEL_ROLE_VS_ATTACH, cfg = c)); } } }
test tcDelRoleVsAttachMain [main=TcDelRoleVsAttachMain]:
  assert DeleteNeedsEmpty in (union System, { TcDelRoleVsAttachMain });
machine TcDelRoleVsAttachById { start state S { entry { var c: tCfg; c = Main(); c.roleDeleteById = true; new Scenario((sc = SC_DEL_ROLE_VS_ATTACH, cfg = c)); } } }
test tcDelRoleVsAttachById [main=TcDelRoleVsAttachById]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch in (union System, { TcDelRoleVsAttachById });
machine TcDelRoleRecreateMain { start state S { entry { var c: tCfg; c = Main();  new Scenario((sc = SC_DEL_ROLE_RECREATE, cfg = c)); } } }
test tcDelRoleRecreateMain [main=TcDelRoleRecreateMain]:
  assert DeleteNeedsEmpty in (union System, { TcDelRoleRecreateMain });
machine TcDelRoleRecreateById { start state S { entry { var c: tCfg; c = Main(); c.roleDeleteById = true; c.roleWriteNeedsRole = true; new Scenario((sc = SC_DEL_ROLE_RECREATE, cfg = c)); } } }
test tcDelRoleRecreateById [main=TcDelRoleRecreateById]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, NamesUnique in (union System, { TcDelRoleRecreateById });
machine TcRoleUpdates { start state S { entry { var c: tCfg; c = Main();  new Scenario((sc = SC_ROLE_UPDATES, cfg = c)); } } }
test tcRoleUpdates [main=TcRoleUpdates]:
  assert NoLostUpdate, IndexesMatch, IamAnswers in (union System, { TcRoleUpdates });
machine TcRoleRetriesOut { start state S { entry { var c: tCfg; c = Main(); c.maxRetries = 0; new Scenario((sc = SC_ROLE_RETRIES, cfg = c)); } } }
test tcRoleRetriesOut [main=TcRoleRetriesOut]:
  assert IamAnswers in (union System, { TcRoleRetriesOut });
machine TcRolesLimitMain { start state S { entry { var c: tCfg; c = Main(); c.maxRoles = 2; new Scenario((sc = SC_ROLES_LIMIT, cfg = c)); } } }
test tcRolesLimitMain [main=TcRolesLimitMain]:
  assert LimitsHold in (union System, { TcRolesLimitMain });
machine TcRolesLimitAtomic { start state S { entry { var c: tCfg; c = Main(); c.maxRoles = 2; c.limitAtomic = true; new Scenario((sc = SC_ROLES_LIMIT, cfg = c)); } } }
test tcRolesLimitAtomic [main=TcRolesLimitAtomic]:
  assert LimitsHold, IndexesMatch, IamAnswers in (union System, { TcRolesLimitAtomic });
machine TcSameRole { start state S { entry { var c: tCfg; c = Main();  new Scenario((sc = SC_SAME_ROLE, cfg = c)); } } }
test tcSameRole [main=TcSameRole]:
  assert NamesUnique, IndexesMatch, IamAnswers in (union System, { TcSameRole });
machine TcRoleCrash { start state S { entry { var c: tCfg; c = Main(); c.mayCrash = true; new Scenario((sc = SC_ROLE_CRASH, cfg = c)); } } }
test tcRoleCrash [main=TcRoleCrash]:
  assert IndexesMatch in (union System, { TcRoleCrash });
// users: DeleteUser racing an update
machine TcDelUserVsKeyMain { start state S { entry { var c: tCfg; c = Main();  new Scenario((sc = SC_DEL_USER_VS_KEY, cfg = c)); } } }
test tcDelUserVsKeyMain [main=TcDelUserVsKeyMain]:
  assert DeletesTakeEffect in (union System, { TcDelUserVsKeyMain });
machine TcDelUserVsKeyIndex { start state S { entry { var c: tCfg; c = Main();  new Scenario((sc = SC_DEL_USER_VS_KEY, cfg = c)); } } }
test tcDelUserVsKeyIndex [main=TcDelUserVsKeyIndex]:
  assert IndexesMatch in (union System, { TcDelUserVsKeyIndex });
machine TcDelUserVsKeyGuarded { start state S { entry { var c: tCfg; c = Main(); c.userDeleteGuarded = true; new Scenario((sc = SC_DEL_USER_VS_KEY, cfg = c)); } } }
test tcDelUserVsKeyGuarded [main=TcDelUserVsKeyGuarded]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, IamAnswers in (union System, { TcDelUserVsKeyGuarded });
machine TcDelUserVsPolicyMain { start state S { entry { var c: tCfg; c = Main();  new Scenario((sc = SC_DEL_USER_VS_POLICY, cfg = c)); } } }
test tcDelUserVsPolicyMain [main=TcDelUserVsPolicyMain]:
  assert DeletesTakeEffect in (union System, { TcDelUserVsPolicyMain });
machine TcDelUserVsPolicyGuarded { start state S { entry { var c: tCfg; c = Main(); c.userDeleteGuarded = true; new Scenario((sc = SC_DEL_USER_VS_POLICY, cfg = c)); } } }
test tcDelUserVsPolicyGuarded [main=TcDelUserVsPolicyGuarded]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, IamAnswers in (union System, { TcDelUserVsPolicyGuarded });
// users: limits and names
machine TcUsersLimitMain { start state S { entry { var c: tCfg; c = Main(); c.maxUsers = 2; new Scenario((sc = SC_USERS_LIMIT, cfg = c)); } } }
test tcUsersLimitMain [main=TcUsersLimitMain]:
  assert LimitsHold in (union System, { TcUsersLimitMain });
machine TcUsersLimitAtomic { start state S { entry { var c: tCfg; c = Main(); c.maxUsers = 2; c.limitAtomic = true; new Scenario((sc = SC_USERS_LIMIT, cfg = c)); } } }
test tcUsersLimitAtomic [main=TcUsersLimitAtomic]:
  assert LimitsHold, IndexesMatch, IamAnswers in (union System, { TcUsersLimitAtomic });
machine TcSameUserMain { start state S { entry { var c: tCfg; c = Main();  new Scenario((sc = SC_SAME_USER, cfg = c)); } } }
test tcSameUserMain [main=TcSameUserMain]:
  assert NamesUnique in (union System, { TcSameUserMain });
machine TcSameUserIndex { start state S { entry { var c: tCfg; c = Main();  new Scenario((sc = SC_SAME_USER, cfg = c)); } } }
test tcSameUserIndex [main=TcSameUserIndex]:
  assert IndexesMatch in (union System, { TcSameUserIndex });
machine TcSameUserAtomic { start state S { entry { var c: tCfg; c = Main(); c.limitAtomic = true; new Scenario((sc = SC_SAME_USER, cfg = c)); } } }
test tcSameUserAtomic [main=TcSameUserAtomic]:
  assert NamesUnique, IndexesMatch, IamAnswers in (union System, { TcSameUserAtomic });
machine TcRenameSameMain { start state S { entry { var c: tCfg; c = Main();  new Scenario((sc = SC_RENAME_SAME, cfg = c)); } } }
test tcRenameSameMain [main=TcRenameSameMain]:
  assert NamesUnique in (union System, { TcRenameSameMain });
machine TcRenameSameAtomic { start state S { entry { var c: tCfg; c = Main(); c.limitAtomic = true; new Scenario((sc = SC_RENAME_SAME, cfg = c)); } } }
test tcRenameSameAtomic [main=TcRenameSameAtomic]:
  assert NamesUnique, IndexesMatch, IamAnswers in (union System, { TcRenameSameAtomic });
machine TcKeys { start state S { entry { var c: tCfg; c = Main();  new Scenario((sc = SC_KEYS, cfg = c)); } } }
test tcKeys [main=TcKeys]:
  assert LimitsHold, NoLostUpdate, IndexesMatch in (union System, { TcKeys });
machine TcUserUpdates { start state S { entry { var c: tCfg; c = Main();  new Scenario((sc = SC_USER_UPDATES, cfg = c)); } } }
test tcUserUpdates [main=TcUserUpdates]:
  assert NoLostUpdate, IndexesMatch, IamAnswers in (union System, { TcUserUpdates });
machine TcUserCrash { start state S { entry { var c: tCfg; c = Main(); c.mayCrash = true; new Scenario((sc = SC_USER_CRASH, cfg = c)); } } }
test tcUserCrash [main=TcUserCrash]:
  assert IndexesMatch in (union System, { TcUserCrash });
// access keys: deactivation racing a user write
machine TcDeactivateMain { start state S { entry { var c: tCfg; c = Main();  new Scenario((sc = SC_DEACTIVATE_VS_POLICY, cfg = c)); } } }
test tcDeactivateMain [main=TcDeactivateMain]:
  assert KeysHonored in (union System, { TcDeactivateMain });
machine TcDeactivateIndex { start state S { entry { var c: tCfg; c = Main();  new Scenario((sc = SC_DEACTIVATE_VS_POLICY, cfg = c)); } } }
test tcDeactivateIndex [main=TcDeactivateIndex]:
  assert IndexesMatch in (union System, { TcDeactivateIndex });
machine TcDeactivateChecked { start state S { entry { var c: tCfg; c = Main(); c.authChecksActive = true; new Scenario((sc = SC_DEACTIVATE_VS_POLICY, cfg = c)); } } }
test tcDeactivateChecked [main=TcDeactivateChecked]:
  assert KeysHonored in (union System, { TcDeactivateChecked });
machine TcDeactivatePassOld { start state S { entry { var c: tCfg; c = Main(); c.policyOpsPassOld = true; new Scenario((sc = SC_DEACTIVATE_VS_POLICY, cfg = c)); } } }
test tcDeactivatePassOld [main=TcDeactivatePassOld]:
  assert KeysHonored, IndexesMatch in (union System, { TcDeactivatePassOld });
// groups
machine TcDelUserInGroup { start state S { entry { var c: tCfg; c = Main();  new Scenario((sc = SC_DEL_USER_IN_GROUP, cfg = c)); } } }
test tcDelUserInGroup [main=TcDelUserInGroup]:
  assert DeleteUserNeedsNoGroups in (union System, { TcDelUserInGroup });
machine TcDelGroupVsAddMain { start state S { entry { var c: tCfg; c = Main();  new Scenario((sc = SC_DEL_GROUP_VS_ADD, cfg = c)); } } }
test tcDelGroupVsAddMain [main=TcDelGroupVsAddMain]:
  assert DeleteNeedsEmpty in (union System, { TcDelGroupVsAddMain });
machine TcDelGroupVsAddIndex { start state S { entry { var c: tCfg; c = Main();  new Scenario((sc = SC_DEL_GROUP_VS_ADD, cfg = c)); } } }
test tcDelGroupVsAddIndex [main=TcDelGroupVsAddIndex]:
  assert IndexesMatch in (union System, { TcDelGroupVsAddIndex });
machine TcDelGroupVsAddGuarded { start state S { entry { var c: tCfg; c = Main(); c.groupLinkGuarded = true; new Scenario((sc = SC_DEL_GROUP_VS_ADD, cfg = c)); } } }
test tcDelGroupVsAddGuarded [main=TcDelGroupVsAddGuarded]:
  assert DeleteNeedsEmpty, IndexesMatch, IamAnswers in (union System, { TcDelGroupVsAddGuarded });
machine TcSameGroupMain { start state S { entry { var c: tCfg; c = Main();  new Scenario((sc = SC_SAME_GROUP, cfg = c)); } } }
test tcSameGroupMain [main=TcSameGroupMain]:
  assert NamesUnique in (union System, { TcSameGroupMain });
machine TcSameGroupAtomic { start state S { entry { var c: tCfg; c = Main(); c.limitAtomic = true; new Scenario((sc = SC_SAME_GROUP, cfg = c)); } } }
test tcSameGroupAtomic [main=TcSameGroupAtomic]:
  assert NamesUnique, IndexesMatch, IamAnswers in (union System, { TcSameGroupAtomic });
machine TcGroupsLimitMain { start state S { entry { var c: tCfg; c = Main(); c.maxGroups = 2; new Scenario((sc = SC_GROUPS_LIMIT, cfg = c)); } } }
test tcGroupsLimitMain [main=TcGroupsLimitMain]:
  assert LimitsHold in (union System, { TcGroupsLimitMain });
machine TcGroupsLimitAtomic { start state S { entry { var c: tCfg; c = Main(); c.maxGroups = 2; c.limitAtomic = true; new Scenario((sc = SC_GROUPS_LIMIT, cfg = c)); } } }
test tcGroupsLimitAtomic [main=TcGroupsLimitAtomic]:
  assert LimitsHold, IndexesMatch, IamAnswers in (union System, { TcGroupsLimitAtomic });
machine TcRenameMemberMain { start state S { entry { var c: tCfg; c = Main();  new Scenario((sc = SC_RENAME_MEMBER, cfg = c)); } } }
test tcRenameMemberMain [main=TcRenameMemberMain]:
  assert IndexesMatch in (union System, { TcRenameMemberMain });
machine TcRenameMemberMoves { start state S { entry { var c: tCfg; c = Main(); c.renameMovesMembers = true; new Scenario((sc = SC_RENAME_MEMBER, cfg = c)); } } }
test tcRenameMemberMoves [main=TcRenameMemberMoves]:
  assert IndexesMatch in (union System, { TcRenameMemberMoves });
machine TcStaleMemberMain { start state S { entry { var c: tCfg; c = Main();  new Scenario((sc = SC_STALE_MEMBER, cfg = c)); } } }
test tcStaleMemberMain [main=TcStaleMemberMain]:
  assert DeleteNeedsEmpty in (union System, { TcStaleMemberMain });
machine TcStaleMemberGuarded { start state S { entry { var c: tCfg; c = Main(); c.groupLinkGuarded = true; new Scenario((sc = SC_STALE_MEMBER, cfg = c)); } } }
test tcStaleMemberGuarded [main=TcStaleMemberGuarded]:
  assert DeleteNeedsEmpty in (union System, { TcStaleMemberGuarded });
machine TcMemberships { start state S { entry { var c: tCfg; c = Main();  new Scenario((sc = SC_MEMBERSHIPS, cfg = c)); } } }
test tcMemberships [main=TcMemberships]:
  assert NoLostUpdate, IndexesMatch, IamAnswers in (union System, { TcMemberships });
// STS
machine TcAssumeVsRecreateMain { start state S { entry { var c: tCfg; c = Main();  new Scenario((sc = SC_ASSUME_VS_RECREATE, cfg = c)); } } }
test tcAssumeVsRecreateMain [main=TcAssumeVsRecreateMain]:
  assert TrustHonored in (union System, { TcAssumeVsRecreateMain });
machine TcAssumeVsRecreateOneRead { start state S { entry { var c: tCfg; c = Main(); c.assumeRoleOneRead = true; new Scenario((sc = SC_ASSUME_VS_RECREATE, cfg = c)); } } }
test tcAssumeVsRecreateOneRead [main=TcAssumeVsRecreateOneRead]:
  assert TrustHonored in (union System, { TcAssumeVsRecreateOneRead });
machine TcAssumeVsTrust { start state S { entry { var c: tCfg; c = Main();  new Scenario((sc = SC_ASSUME_VS_TRUST, cfg = c)); } } }
test tcAssumeVsTrust [main=TcAssumeVsTrust]:
  assert TrustHonored, IamAnswers in (union System, { TcAssumeVsTrust });
machine TcAssumeIdentityOnlyMain { start state S { entry { var c: tCfg; c = Main();  new Scenario((sc = SC_ASSUME_IDENTITY_ONLY, cfg = c)); } } }
test tcAssumeIdentityOnlyMain [main=TcAssumeIdentityOnlyMain]:
  assert TrustHonored in (union System, { TcAssumeIdentityOnlyMain });
machine TcAssumeIdentityOnlyAws { start state S { entry { var c: tCfg; c = Main(); c.trustRequired = true; new Scenario((sc = SC_ASSUME_IDENTITY_ONLY, cfg = c)); } } }
test tcAssumeIdentityOnlyAws [main=TcAssumeIdentityOnlyAws]:
  assert TrustHonored, IamAnswers in (union System, { TcAssumeIdentityOnlyAws });
machine TcAssumeNoRoleMain { start state S { entry { var c: tCfg; c = Main();  new Scenario((sc = SC_ASSUME_NO_ROLE, cfg = c)); } } }
test tcAssumeNoRoleMain [main=TcAssumeNoRoleMain]:
  assert IamAnswers in (union System, { TcAssumeNoRoleMain });
machine TcAssumeNoRoleDenied { start state S { entry { var c: tCfg; c = Main(); c.assumeDeniesMissing = true; new Scenario((sc = SC_ASSUME_NO_ROLE, cfg = c)); } } }
test tcAssumeNoRoleDenied [main=TcAssumeNoRoleDenied]:
  assert IamAnswers in (union System, { TcAssumeNoRoleDenied });
machine TcSessionAfterDelete { start state S { entry { var c: tCfg; c = Main();  new Scenario((sc = SC_SESSION_AFTER_DELETE, cfg = c)); } } }
test tcSessionAfterDelete [main=TcSessionAfterDelete]:
  assert SessionsDieWithRole, IamAnswers in (union System, { TcSessionAfterDelete });
machine TcGstFromRoleMain { start state S { entry { var c: tCfg; c = Main();  new Scenario((sc = SC_GST_FROM_ROLE, cfg = c)); } } }
test tcGstFromRoleMain [main=TcGstFromRoleMain]:
  assert SessionsDieWithRole in (union System, { TcGstFromRoleMain });
machine TcGstFromRoleAns { start state S { entry { var c: tCfg; c = Main();  new Scenario((sc = SC_GST_FROM_ROLE, cfg = c)); } } }
test tcGstFromRoleAns [main=TcGstFromRoleAns]:
  assert IamAnswers in (union System, { TcGstFromRoleAns });
machine TcGstFromRoleLongTerm { start state S { entry { var c: tCfg; c = Main(); c.gstLongTermOnly = true; new Scenario((sc = SC_GST_FROM_ROLE, cfg = c)); } } }
test tcGstFromRoleLongTerm [main=TcGstFromRoleLongTerm]:
  assert SessionsDieWithRole, IamAnswers in (union System, { TcGstFromRoleLongTerm });
// every proposed fix
machine TcFixDelRoleVsPut { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; new Scenario((sc = SC_DEL_ROLE_VS_PUT, cfg = c)); } } }
test tcFixDelRoleVsPut [main=TcFixDelRoleVsPut]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcFixDelRoleVsPut });
machine TcFixDelRoleVsAttach { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; new Scenario((sc = SC_DEL_ROLE_VS_ATTACH, cfg = c)); } } }
test tcFixDelRoleVsAttach [main=TcFixDelRoleVsAttach]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcFixDelRoleVsAttach });
machine TcFixDelRoleRecreate { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; new Scenario((sc = SC_DEL_ROLE_RECREATE, cfg = c)); } } }
test tcFixDelRoleRecreate [main=TcFixDelRoleRecreate]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcFixDelRoleRecreate });
machine TcFixRoleUpdates { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; new Scenario((sc = SC_ROLE_UPDATES, cfg = c)); } } }
test tcFixRoleUpdates [main=TcFixRoleUpdates]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcFixRoleUpdates });
machine TcFixRolesLimit { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; new Scenario((sc = SC_ROLES_LIMIT, cfg = c)); } } }
test tcFixRolesLimit [main=TcFixRolesLimit]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcFixRolesLimit });
machine TcFixSameRole { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; new Scenario((sc = SC_SAME_ROLE, cfg = c)); } } }
test tcFixSameRole [main=TcFixSameRole]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcFixSameRole });
machine TcFixDelUserVsKey { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; new Scenario((sc = SC_DEL_USER_VS_KEY, cfg = c)); } } }
test tcFixDelUserVsKey [main=TcFixDelUserVsKey]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcFixDelUserVsKey });
machine TcFixDelUserVsPolicy { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; new Scenario((sc = SC_DEL_USER_VS_POLICY, cfg = c)); } } }
test tcFixDelUserVsPolicy [main=TcFixDelUserVsPolicy]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcFixDelUserVsPolicy });
machine TcFixUsersLimit { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; new Scenario((sc = SC_USERS_LIMIT, cfg = c)); } } }
test tcFixUsersLimit [main=TcFixUsersLimit]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcFixUsersLimit });
machine TcFixSameUser { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; new Scenario((sc = SC_SAME_USER, cfg = c)); } } }
test tcFixSameUser [main=TcFixSameUser]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcFixSameUser });
machine TcFixRenameSame { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; new Scenario((sc = SC_RENAME_SAME, cfg = c)); } } }
test tcFixRenameSame [main=TcFixRenameSame]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcFixRenameSame });
machine TcFixKeys { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; new Scenario((sc = SC_KEYS, cfg = c)); } } }
test tcFixKeys [main=TcFixKeys]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcFixKeys });
machine TcFixDeactivateVsPolicy { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; new Scenario((sc = SC_DEACTIVATE_VS_POLICY, cfg = c)); } } }
test tcFixDeactivateVsPolicy [main=TcFixDeactivateVsPolicy]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcFixDeactivateVsPolicy });
machine TcFixUserUpdates { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; new Scenario((sc = SC_USER_UPDATES, cfg = c)); } } }
test tcFixUserUpdates [main=TcFixUserUpdates]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcFixUserUpdates });
machine TcFixDelGroupVsAdd { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; new Scenario((sc = SC_DEL_GROUP_VS_ADD, cfg = c)); } } }
test tcFixDelGroupVsAdd [main=TcFixDelGroupVsAdd]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcFixDelGroupVsAdd });
machine TcFixSameGroup { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; new Scenario((sc = SC_SAME_GROUP, cfg = c)); } } }
test tcFixSameGroup [main=TcFixSameGroup]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcFixSameGroup });
machine TcFixGroupsLimit { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; new Scenario((sc = SC_GROUPS_LIMIT, cfg = c)); } } }
test tcFixGroupsLimit [main=TcFixGroupsLimit]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcFixGroupsLimit });
machine TcFixStaleMember { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; new Scenario((sc = SC_STALE_MEMBER, cfg = c)); } } }
test tcFixStaleMember [main=TcFixStaleMember]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcFixStaleMember });
machine TcFixMemberships { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; new Scenario((sc = SC_MEMBERSHIPS, cfg = c)); } } }
test tcFixMemberships [main=TcFixMemberships]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcFixMemberships });
machine TcFixAssumeVsRecreate { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; new Scenario((sc = SC_ASSUME_VS_RECREATE, cfg = c)); } } }
test tcFixAssumeVsRecreate [main=TcFixAssumeVsRecreate]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcFixAssumeVsRecreate });
machine TcFixAssumeVsTrust { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; new Scenario((sc = SC_ASSUME_VS_TRUST, cfg = c)); } } }
test tcFixAssumeVsTrust [main=TcFixAssumeVsTrust]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcFixAssumeVsTrust });
machine TcFixAssumeIdentityOnly { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; new Scenario((sc = SC_ASSUME_IDENTITY_ONLY, cfg = c)); } } }
test tcFixAssumeIdentityOnly [main=TcFixAssumeIdentityOnly]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcFixAssumeIdentityOnly });
machine TcFixSessionAfterDelete { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; new Scenario((sc = SC_SESSION_AFTER_DELETE, cfg = c)); } } }
test tcFixSessionAfterDelete [main=TcFixSessionAfterDelete]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcFixSessionAfterDelete });
machine TcFixGstFromRole { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; new Scenario((sc = SC_GST_FROM_ROLE, cfg = c)); } } }
test tcFixGstFromRole [main=TcFixGstFromRole]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcFixGstFromRole });
machine TcFixRenameMember { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; new Scenario((sc = SC_RENAME_MEMBER, cfg = c)); } } }
test tcFixRenameMember [main=TcFixRenameMember]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcFixRenameMember });
machine TcFixAssumeNoRole { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; new Scenario((sc = SC_ASSUME_NO_ROLE, cfg = c)); } } }
test tcFixAssumeNoRole [main=TcFixAssumeNoRole]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcFixAssumeNoRole });
machine TcFixDelUserInGroup { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; new Scenario((sc = SC_DEL_USER_IN_GROUP, cfg = c)); } } }
test tcFixDelUserInGroup [main=TcFixDelUserInGroup]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcFixDelUserInGroup });

// #81353: DeleteUser of a user in a group (userDeleteNeedsNoGroups)
machine TcNoGroupsInGroup { start state S { entry { var c: tCfg; c = Main(); c.userDeleteNeedsNoGroups = true; new Scenario((sc = SC_DEL_USER_IN_GROUP, cfg = c)); } } }
test tcNoGroupsInGroup [main=TcNoGroupsInGroup]:
  assert DeleteUserNeedsNoGroups, IndexesMatch, IamAnswers in (union System, { TcNoGroupsInGroup });
machine TcDelUserVsAddMain { start state S { entry { var c: tCfg; c = Main();  new Scenario((sc = SC_DEL_USER_VS_ADD, cfg = c)); } } }
test tcDelUserVsAddMain [main=TcDelUserVsAddMain]:
  assert DeletesTakeEffect in (union System, { TcDelUserVsAddMain });
machine TcDelUserVsAddNoGroups { start state S { entry { var c: tCfg; c = Main(); c.userDeleteNeedsNoGroups = true; new Scenario((sc = SC_DEL_USER_VS_ADD, cfg = c)); } } }
test tcDelUserVsAddNoGroups [main=TcDelUserVsAddNoGroups]:
  assert DeleteUserNeedsNoGroups, DeletesTakeEffect in (union System, { TcDelUserVsAddNoGroups });
machine TcDelUserVsAddGuarded { start state S { entry { var c: tCfg; c = Main(); c.userDeleteNeedsNoGroups = true; c.userDeleteGuarded = true; new Scenario((sc = SC_DEL_USER_VS_ADD, cfg = c)); } } }
test tcDelUserVsAddGuarded [main=TcDelUserVsAddGuarded]:
  assert DeleteUserNeedsNoGroups, DeletesTakeEffect, IndexesMatch, IamAnswers in (union System, { TcDelUserVsAddGuarded });
machine TcFixDelUserVsAdd { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; c.userDeleteNeedsNoGroups = true; new Scenario((sc = SC_DEL_USER_VS_ADD, cfg = c)); } } }
test tcFixDelUserVsAdd [main=TcFixDelUserVsAdd]:
  assert DeleteUserNeedsNoGroups, DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcFixDelUserVsAdd });
machine TcFixNoGroupsInGroup { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; c.userDeleteNeedsNoGroups = true; new Scenario((sc = SC_DEL_USER_IN_GROUP, cfg = c)); } } }
test tcFixNoGroupsInGroup [main=TcFixNoGroupsInGroup]:
  assert DeleteUserNeedsNoGroups, DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcFixNoGroupsInGroup });

// #81354: retries. A request loses a race only to another request's write, so n
// concurrent writers of one entity need n - 1 retries at most
machine TcRoleHotOneRetry { start state S { entry { var c: tCfg; c = Main(); c.maxRetries = 1; new Scenario((sc = SC_ROLE_HOT, cfg = c)); } } }
test tcRoleHotOneRetry [main=TcRoleHotOneRetry]:
  assert IamAnswers in (union System, { TcRoleHotOneRetry });
machine TcRoleHotTwoRetries { start state S { entry { var c: tCfg; c = Main(); c.maxRetries = 2; new Scenario((sc = SC_ROLE_HOT, cfg = c)); } } }
test tcRoleHotTwoRetries [main=TcRoleHotTwoRetries]:
  assert NoLostUpdate, IndexesMatch, IamAnswers in (union System, { TcRoleHotTwoRetries });
machine TcRoleHotUnlimited { start state S { entry { var c: tCfg; c = Main(); c.maxRetries = -1; new Scenario((sc = SC_ROLE_HOT, cfg = c)); } } }
test tcRoleHotUnlimited [main=TcRoleHotUnlimited]:
  assert NoLostUpdate, IndexesMatch, IamAnswers in (union System, { TcRoleHotUnlimited });
machine TcRoleFailsAfterRetries { start state S { entry { var c: tCfg; c = Main(); c.maxRetries = 0; c.retriesOutServiceFailure = true; new Scenario((sc = SC_ROLE_RETRIES, cfg = c)); } } }
test tcRoleFailsAfterRetries [main=TcRoleFailsAfterRetries]:
  assert IndexesMatch, IamAnswers in (union System, { TcRoleFailsAfterRetries });
machine TcFixRoleHot { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; c.maxRetries = 1; c.retriesOutServiceFailure = true; new Scenario((sc = SC_ROLE_HOT, cfg = c)); } } }
test tcFixRoleHot [main=TcFixRoleHot]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcFixRoleHot });

// every proposed fix, with only the ops RADOS and cls_user offer today
machine TcExistingDelRoleVsPut { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; c.existingOpsOnly = true; new Scenario((sc = SC_DEL_ROLE_VS_PUT, cfg = c)); } } }
test tcExistingDelRoleVsPut [main=TcExistingDelRoleVsPut]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcExistingDelRoleVsPut });
machine TcExistingDelRoleVsAttach { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; c.existingOpsOnly = true; new Scenario((sc = SC_DEL_ROLE_VS_ATTACH, cfg = c)); } } }
test tcExistingDelRoleVsAttach [main=TcExistingDelRoleVsAttach]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcExistingDelRoleVsAttach });
machine TcExistingDelRoleRecreate { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; c.existingOpsOnly = true; new Scenario((sc = SC_DEL_ROLE_RECREATE, cfg = c)); } } }
test tcExistingDelRoleRecreate [main=TcExistingDelRoleRecreate]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcExistingDelRoleRecreate });
machine TcExistingRoleUpdates { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; c.existingOpsOnly = true; new Scenario((sc = SC_ROLE_UPDATES, cfg = c)); } } }
test tcExistingRoleUpdates [main=TcExistingRoleUpdates]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcExistingRoleUpdates });
machine TcExistingRolesLimit { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; c.existingOpsOnly = true; new Scenario((sc = SC_ROLES_LIMIT, cfg = c)); } } }
test tcExistingRolesLimit [main=TcExistingRolesLimit]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcExistingRolesLimit });
machine TcExistingSameRole { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; c.existingOpsOnly = true; new Scenario((sc = SC_SAME_ROLE, cfg = c)); } } }
test tcExistingSameRole [main=TcExistingSameRole]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcExistingSameRole });
machine TcExistingDelUserVsKey { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; c.existingOpsOnly = true; new Scenario((sc = SC_DEL_USER_VS_KEY, cfg = c)); } } }
test tcExistingDelUserVsKey [main=TcExistingDelUserVsKey]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcExistingDelUserVsKey });
machine TcExistingDelUserVsPolicy { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; c.existingOpsOnly = true; new Scenario((sc = SC_DEL_USER_VS_POLICY, cfg = c)); } } }
test tcExistingDelUserVsPolicy [main=TcExistingDelUserVsPolicy]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcExistingDelUserVsPolicy });
machine TcExistingUsersLimit { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; c.existingOpsOnly = true; new Scenario((sc = SC_USERS_LIMIT, cfg = c)); } } }
test tcExistingUsersLimit [main=TcExistingUsersLimit]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcExistingUsersLimit });
machine TcExistingSameUser { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; c.existingOpsOnly = true; new Scenario((sc = SC_SAME_USER, cfg = c)); } } }
test tcExistingSameUser [main=TcExistingSameUser]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcExistingSameUser });
machine TcExistingRenameSame { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; c.existingOpsOnly = true; new Scenario((sc = SC_RENAME_SAME, cfg = c)); } } }
test tcExistingRenameSame [main=TcExistingRenameSame]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcExistingRenameSame });
machine TcExistingKeys { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; c.existingOpsOnly = true; new Scenario((sc = SC_KEYS, cfg = c)); } } }
test tcExistingKeys [main=TcExistingKeys]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcExistingKeys });
machine TcExistingDeactivateVsPolicy { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; c.existingOpsOnly = true; new Scenario((sc = SC_DEACTIVATE_VS_POLICY, cfg = c)); } } }
test tcExistingDeactivateVsPolicy [main=TcExistingDeactivateVsPolicy]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcExistingDeactivateVsPolicy });
machine TcExistingUserUpdates { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; c.existingOpsOnly = true; new Scenario((sc = SC_USER_UPDATES, cfg = c)); } } }
test tcExistingUserUpdates [main=TcExistingUserUpdates]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcExistingUserUpdates });
machine TcExistingDelGroupVsAdd { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; c.existingOpsOnly = true; new Scenario((sc = SC_DEL_GROUP_VS_ADD, cfg = c)); } } }
test tcExistingDelGroupVsAdd [main=TcExistingDelGroupVsAdd]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcExistingDelGroupVsAdd });
machine TcExistingSameGroup { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; c.existingOpsOnly = true; new Scenario((sc = SC_SAME_GROUP, cfg = c)); } } }
test tcExistingSameGroup [main=TcExistingSameGroup]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcExistingSameGroup });
machine TcExistingGroupsLimit { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; c.existingOpsOnly = true; new Scenario((sc = SC_GROUPS_LIMIT, cfg = c)); } } }
test tcExistingGroupsLimit [main=TcExistingGroupsLimit]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcExistingGroupsLimit });
machine TcExistingStaleMember { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; c.existingOpsOnly = true; new Scenario((sc = SC_STALE_MEMBER, cfg = c)); } } }
test tcExistingStaleMember [main=TcExistingStaleMember]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcExistingStaleMember });
machine TcExistingMemberships { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; c.existingOpsOnly = true; new Scenario((sc = SC_MEMBERSHIPS, cfg = c)); } } }
test tcExistingMemberships [main=TcExistingMemberships]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcExistingMemberships });
machine TcExistingAssumeVsRecreate { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; c.existingOpsOnly = true; new Scenario((sc = SC_ASSUME_VS_RECREATE, cfg = c)); } } }
test tcExistingAssumeVsRecreate [main=TcExistingAssumeVsRecreate]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcExistingAssumeVsRecreate });
machine TcExistingAssumeVsTrust { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; c.existingOpsOnly = true; new Scenario((sc = SC_ASSUME_VS_TRUST, cfg = c)); } } }
test tcExistingAssumeVsTrust [main=TcExistingAssumeVsTrust]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcExistingAssumeVsTrust });
machine TcExistingAssumeIdentityOnly { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; c.existingOpsOnly = true; new Scenario((sc = SC_ASSUME_IDENTITY_ONLY, cfg = c)); } } }
test tcExistingAssumeIdentityOnly [main=TcExistingAssumeIdentityOnly]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcExistingAssumeIdentityOnly });
machine TcExistingSessionAfterDelete { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; c.existingOpsOnly = true; new Scenario((sc = SC_SESSION_AFTER_DELETE, cfg = c)); } } }
test tcExistingSessionAfterDelete [main=TcExistingSessionAfterDelete]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcExistingSessionAfterDelete });
machine TcExistingGstFromRole { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; c.existingOpsOnly = true; new Scenario((sc = SC_GST_FROM_ROLE, cfg = c)); } } }
test tcExistingGstFromRole [main=TcExistingGstFromRole]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcExistingGstFromRole });
machine TcExistingRenameMember { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; c.existingOpsOnly = true; new Scenario((sc = SC_RENAME_MEMBER, cfg = c)); } } }
test tcExistingRenameMember [main=TcExistingRenameMember]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcExistingRenameMember });
machine TcExistingAssumeNoRole { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; c.existingOpsOnly = true; new Scenario((sc = SC_ASSUME_NO_ROLE, cfg = c)); } } }
test tcExistingAssumeNoRole [main=TcExistingAssumeNoRole]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcExistingAssumeNoRole });
machine TcExistingDelUserInGroup { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; c.existingOpsOnly = true; new Scenario((sc = SC_DEL_USER_IN_GROUP, cfg = c)); } } }
test tcExistingDelUserInGroup [main=TcExistingDelUserInGroup]:
  assert DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcExistingDelUserInGroup });
machine TcExistingDelUserVsAdd { start state S { entry { var c: tCfg; c = Main(); c = Fixed(); c.maxRoles = 2; c.maxUsers = 3; c.maxGroups = 3; c.userDeleteNeedsNoGroups = true; c.existingOpsOnly = true; new Scenario((sc = SC_DEL_USER_VS_ADD, cfg = c)); } } }
test tcExistingDelUserVsAdd [main=TcExistingDelUserVsAdd]:
  assert DeleteUserNeedsNoGroups, DeleteNeedsEmpty, DeletesTakeEffect, IndexesMatch, LimitsHold, NamesUnique, NoLostUpdate, KeysHonored, TrustHonored, SessionsDieWithRole, IamAnswers in (union System, { TcExistingDelUserVsAdd });
