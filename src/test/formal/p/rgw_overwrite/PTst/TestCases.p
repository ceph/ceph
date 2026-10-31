module System = { Scenario, Driver, Store, Rgw };

// PutObject over PutObject
test tcPutsSafe [main=TestPuts]:
  assert HeadIntact, NoOrphans, AllAnswered, BucketStats in (union System, { TestPuts });
test tcPutsIndex [main=TestPuts]:
  assert IndexMatchesHead in (union System, { TestPuts });
test tcPutsCancelKeepsVer [main=TestPutsCancelKeepsVer]:
  assert IndexMatchesHead in (union System, { TestPutsCancelKeepsVer });
test tcBugNoIdTagGuard [main=TestPutsNoIdTagGuard]:
  assert NoOrphans in (union System, { TestPutsNoIdTagGuard });

// a completion over the key, racing PutObject or another completion
test tcPutVsCompleteSafe [main=TestPutVsComplete]:
  assert HeadIntact, IndexMatchesHead, AllAnswered, BucketStats in (union System, { TestPutVsComplete });
test tcPutVsCompleteLeak [main=TestPutVsComplete]:
  assert NoOrphans in (union System, { TestPutVsComplete });
test tcPutVsCompleteLoserGc [main=TestPutVsCompleteLoserGc]:
  assert NoOrphans in (union System, { TestPutVsCompleteLoserGc });
test tcBugCancelSkipsRemoveObjs [main=TestPutVsCompleteCancelSkipsRemoveObjs]:
  assert NoOrphans in (union System, { TestPutVsCompleteCancelSkipsRemoveObjs });
test tcCompletesSafe [main=TestCompletes]:
  assert HeadIntact, IndexMatchesHead, AllAnswered, BucketStats in (union System, { TestCompletes });
test tcCompletesLeak [main=TestCompletes]:
  assert NoOrphans in (union System, { TestCompletes });
test tcCompletesLoserGc [main=TestCompletesLoserGc]:
  assert NoOrphans in (union System, { TestCompletesLoserGc });

// a part re-uploaded during the completion
test tcReupload [main=TestReupload]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, AllAnswered, BucketStats in (union System, { TestReupload });
test tcBugNoMetaVersionCheck [main=TestReuploadNoMetaVersionCheck]:
  assert NoOrphans in (union System, { TestReuploadNoMetaVersionCheck });
test tcBugHistoryNoSkip [main=TestReuploadHistoryNoSkip]:
  assert HeadIntact in (union System, { TestReuploadHistoryNoSkip });

// an abort during the completion
test tcAbortVsComplete [main=TestAbort]:
  assert HeadIntact, NoOrphans, AllAnswered, BucketStats in (union System, { TestAbort });
test tcBugAbortNoLock [main=TestAbortNoLock]:
  assert HeadIntact in (union System, { TestAbortNoLock });
test tcAssumeLockHeld [main=TestAbortLockLapses]:
  assert HeadIntact in (union System, { TestAbortLockLapses });
test tcLcAbortVsComplete [main=TestLcAbort]:
  assert HeadIntact in (union System, { TestLcAbort });
test tcLcAbortTakesLock [main=TestLcAbortTakesLock]:
  assert HeadIntact, NoOrphans, AllAnswered in (union System, { TestLcAbortTakesLock });

// one upload completed three times at once
test tcSameCompletes [main=TestSameCompletes]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestSameCompletes });
test tcBugNoLockRenewal [main=TestSameCompletesLockLapses]:
  assert HeadIntact in (union System, { TestSameCompletesLockLapses });
test tcBugReplayNoEtag [main=TestSameCompletesReplayNoEtag]:
  assert CompletionEtag in (union System, { TestSameCompletesReplayNoEtag });

// a completion that leaves its meta object behind, then a retry or an abort
test tcCrashThenRetry [main=TestCrashThenRetry]:
  assert HeadIntact in (union System, { TestCrashThenRetry });
test tcCrashThenAbort [main=TestCrashThenAbort]:
  assert HeadIntact in (union System, { TestCrashThenAbort });
test tcMetaDeleteFailsThenRetry [main=TestMetaDeleteFailsThenRetry]:
  assert HeadIntact in (union System, { TestMetaDeleteFailsThenRetry });
test tcMetaDeleteFailsThenAbort [main=TestMetaDeleteFailsThenAbort]:
  assert HeadIntact in (union System, { TestMetaDeleteFailsThenAbort });
test tcSparesHeadCrashRetry [main=TestCrashThenRetrySparesHead]:
  assert HeadIntact, AllAnswered in (union System, { TestCrashThenRetrySparesHead });
test tcSparesHeadCrashAbort [main=TestCrashThenAbortSparesHead]:
  assert HeadIntact, AllAnswered in (union System, { TestCrashThenAbortSparesHead });
test tcSparesHeadCrashPutRetry [main=TestCrashPutThenRetrySparesHead]:
  assert HeadIntact in (union System, { TestCrashPutThenRetrySparesHead });

// DeleteObject on a non-versioned bucket
test tcDelVsPutSafe [main=TestDelVsPut]:
  assert HeadIntact, IndexMatchesHead, AllAnswered, BucketStats in (union System, { TestDelVsPut });
test tcDelVsPutLeak [main=TestDelVsPut]:
  assert NoOrphans in (union System, { TestDelVsPut });
test tcDelVsPutGuard [main=TestDelVsPutGuard]:
  assert HeadIntact, NoOrphans, IndexMatchesHead in (union System, { TestDelVsPutGuard });
test tcDelsAndPutSafe [main=TestDelsAndPut]:
  assert HeadIntact, AllAnswered, BucketStats in (union System, { TestDelsAndPut });
test tcDelsAndPutIndex [main=TestDelsAndPut]:
  assert IndexMatchesHead in (union System, { TestDelsAndPut });
test tcDelsCancelKeepsVer [main=TestDelsAndPutCancelKeepsVer]:
  assert IndexMatchesHead in (union System, { TestDelsAndPutCancelKeepsVer });
test tcDelVsCompleteSafe [main=TestDelVsComplete]:
  assert HeadIntact, IndexMatchesHead, AllAnswered, BucketStats in (union System, { TestDelVsComplete });
test tcDelVsCompleteLeak [main=TestDelVsComplete]:
  assert NoOrphans in (union System, { TestDelVsComplete });
test tcDelVsCompleteGuard [main=TestDelVsCompleteGuard]:
  assert HeadIntact, NoOrphans in (union System, { TestDelVsCompleteGuard });

// CopyObject sharing the source's tail
test tcCopyVsPutSrc [main=TestCopyVsPutSrc]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, AllAnswered, BucketStats in (union System, { TestCopyVsPutSrc });
test tcBugCopyNoRefs [main=TestCopyVsPutSrcNoRefs]:
  assert HeadIntact in (union System, { TestCopyVsPutSrcNoRefs });
test tcCopyVsDelSrc [main=TestCopyVsDelSrc]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, AllAnswered, BucketStats in (union System, { TestCopyVsDelSrc });
test tcCopyVsPutDstSafe [main=TestCopyVsPutDst]:
  assert HeadIntact, IndexMatchesHead, AllAnswered, BucketStats in (union System, { TestCopyVsPutDst });
test tcCopyVsPutDstLeak [main=TestCopyVsPutDst]:
  assert NoOrphans in (union System, { TestCopyVsPutDst });
test tcCopyVsPutDstDropRefs [main=TestCopyVsPutDstDropRefs]:
  assert NoOrphans in (union System, { TestCopyVsPutDstDropRefs });
test tcCopyThenDeletes [main=TestCopyThenDeletes]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, AllAnswered, BucketStats in (union System, { TestCopyThenDeletes });
test tcCopySelfVsPutLoss [main=TestCopySelfVsPut]:
  assert HeadIntact in (union System, { TestCopySelfVsPut });
test tcCopySelfGuarded [main=TestCopySelfVsPutGuarded]:
  assert HeadIntact, NoOrphans, IndexMatchesHead in (union System, { TestCopySelfVsPutGuarded });
test tcCopyMpu [main=TestCopyMpu]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, AllAnswered, BucketStats in (union System, { TestCopyMpu });
test tcCrashCopyRetry [main=TestCrashCopyRetry]:
  assert HeadIntact in (union System, { TestCrashCopyRetry });

// an index completion that fails after the head write
test tcIxFailPutLoss [main=TestIxFailPut]:
  assert HeadIntact in (union System, { TestIxFailPut });
test tcIxFailPutIndex [main=TestIxFailPut]:
  assert IndexMatchesHead in (union System, { TestIxFailPut });
test tcIxFailCopyLoss [main=TestIxFailCopy]:
  assert HeadIntact in (union System, { TestIxFailCopy });
test tcIxFailRetryLoss [main=TestIxFailRetry]:
  assert HeadIntact in (union System, { TestIxFailRetry });
test tcIxKeepsWritePut [main=TestIxKeepsWritePut]:
  assert HeadIntact, IndexMatchesHead, NoOrphans, BucketStats in (union System, { TestIxKeepsWritePut });
test tcIxKeepsWriteCopy [main=TestIxKeepsWriteCopy]:
  assert HeadIntact, IndexMatchesHead, NoOrphans, BucketStats in (union System, { TestIxKeepsWriteCopy });
test tcIxKeepsWriteRetry [main=TestIxKeepsWriteRetry]:
  assert HeadIntact, IndexMatchesHead in (union System, { TestIxKeepsWriteRetry });

// a bucket listing's repair
test tcListVsPut [main=TestListVsPut]:
  assert HeadIntact, IndexMatchesHead, NoOrphans, BucketStats, AllAnswered in (union System, { TestListVsPut });
test tcListVsDel [main=TestListVsDel]:
  assert HeadIntact, IndexMatchesHead, NoOrphans, BucketStats, AllAnswered in (union System, { TestListVsDel });
test tcListVsComplete [main=TestListVsComplete]:
  assert HeadIntact, IndexMatchesHead, NoOrphans, BucketStats, AllAnswered in (union System, { TestListVsComplete });
test tcAssumeWritersPrompt [main=TestListVsPutSlowWriter]:
  assert IndexMatchesHead in (union System, { TestListVsPutSlowWriter });

// dedup of key 2's object onto key 1's
test tcDedupThenDeletes [main=TestDedupThenDeletes]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, BucketStats, AllAnswered in (union System, { TestDedupThenDeletes });
test tcDedupVsPutTgtSafe [main=TestDedupVsPutTgt]:
  assert HeadIntact, IndexMatchesHead, AllAnswered in (union System, { TestDedupVsPutTgt });
test tcDedupVsPutTgtLeak [main=TestDedupVsPutTgt]:
  assert NoOrphans in (union System, { TestDedupVsPutTgt });
test tcDedupVsDelTgtLeak [main=TestDedupVsDelTgt]:
  assert NoOrphans in (union System, { TestDedupVsDelTgt });
test tcDedupVsDelTgtGuarded [main=TestDedupVsDelTgtGuard]:
  assert NoOrphans in (union System, { TestDedupVsDelTgtGuard });
test tcDedupVsPutSrc [main=TestDedupVsPutSrc]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, BucketStats, AllAnswered in (union System, { TestDedupVsPutSrc });
test tcDedupVsCopySelfLoss [main=TestDedupVsCopySelf]:
  assert HeadIntact in (union System, { TestDedupVsCopySelf });
test tcDedupVsCopySelfGuarded [main=TestDedupVsCopySelfGuarded]:
  assert HeadIntact in (union System, { TestDedupVsCopySelfGuarded });

// a bucket reshard racing writes
test tcReshardVsPuts [main=TestReshardVsPuts]:
  assert HeadIntact, IndexMatchesHead, NoOrphans, BucketStats, AllAnswered in (union System, { TestReshardVsPuts });
test tcReshardVsDel [main=TestReshardVsDel]:
  assert HeadIntact, IndexMatchesHead, BucketStats, AllAnswered in (union System, { TestReshardVsDel });
test tcReshardVsMpu [main=TestReshardVsMpu]:
  assert HeadIntact, IndexMatchesHead, NoOrphans, BucketStats, AllAnswered in (union System, { TestReshardVsMpu });
test tcBugReshardNoLog [main=TestReshardNoLog]:
  assert IndexMatchesHead in (union System, { TestReshardNoLog });
test tcBugReshardNoCheckExisting [main=TestReshardNoCheckExisting]:
  assert BucketStats in (union System, { TestReshardNoCheckExisting });
test tcBugOldShardsOpen [main=TestReshardOldShardsOpen]:
  assert IndexMatchesHead in (union System, { TestReshardOldShardsOpen });

// conditional requests
test tcCreates [main=TestCreates]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics in (union System, { TestCreates });
test tcCreateVsCompleteSafe [main=TestCreateVsComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, AllAnswered, BucketStats in (union System, { TestCreateVsComplete });
test tcCreateVsCompleteCond [main=TestCreateVsComplete]:
  assert CondSemantics in (union System, { TestCreateVsComplete });
test tcCreateVsCompleteKeepsParts [main=TestCreateVsCompleteKeepsParts]:
  assert CondSemantics in (union System, { TestCreateVsCompleteKeepsParts });
test tcIfMatchVsPut [main=TestIfMatchVsPut]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics in (union System, { TestIfMatchVsPut });
test tcBugCondNoIdTagGuard [main=TestIfMatchVsPutNoIdTagGuard]:
  assert CondSemantics in (union System, { TestIfMatchVsPutNoIdTagGuard });
test tcMatchAnyVsMatchSafe [main=TestMatchAnyVsMatch]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, AllAnswered, BucketStats in (union System, { TestMatchAnyVsMatch });
test tcMatchAnyVsMatchCond [main=TestMatchAnyVsMatch]:
  assert CondSemantics in (union System, { TestMatchAnyVsMatch });
test tcMatchAnyVsMatchLossFails [main=TestMatchAnyVsMatchLossFails]:
  assert CondSemantics in (union System, { TestMatchAnyVsMatchLossFails });
test tcCondDelVsPutSafe [main=TestCondDelVsPut]:
  assert HeadIntact, IndexMatchesHead, AllAnswered, BucketStats in (union System, { TestCondDelVsPut });
test tcCondDelVsPutLeak [main=TestCondDelVsPut]:
  assert NoOrphans in (union System, { TestCondDelVsPut });
test tcCondDelVsPutCond [main=TestCondDelVsPut]:
  assert CondSemantics in (union System, { TestCondDelVsPut });
test tcCondDelVsPutGuard [main=TestCondDelVsPutGuard]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CondSemantics in (union System, { TestCondDelVsPutGuard });
test tcCondDelVsMatchSafe [main=TestCondDelVsMatch]:
  assert HeadIntact, IndexMatchesHead, AllAnswered, BucketStats in (union System, { TestCondDelVsMatch });
test tcCondDelVsMatchCond [main=TestCondDelVsMatch]:
  assert CondSemantics in (union System, { TestCondDelVsMatch });
test tcCondDelVsMatchGuard [main=TestCondDelVsMatchGuard]:
  assert CondSemantics in (union System, { TestCondDelVsMatchGuard });
test tcCondDelVsMatchLossFails [main=TestCondDelVsMatchLossFails]:
  assert CondSemantics in (union System, { TestCondDelVsMatchLossFails });
test tcCondCompleteVsPut [main=TestCondCompleteVsPut]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics in (union System, { TestCondCompleteVsPut });

// the fixes together
test tcFixedPuts [main=TestFixedPuts]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedPuts });
test tcFixedPutOne [main=TestFixedPutOne]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedPutOne });
test tcFixedPutVsComplete [main=TestFixedPutVsComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedPutVsComplete });
test tcFixedCompletes [main=TestFixedCompletes]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedCompletes });
test tcFixedSameCompletes [main=TestFixedSameCompletes]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedSameCompletes });
test tcFixedReupload [main=TestFixedReupload]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedReupload });
test tcFixedAbort [main=TestFixedAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedAbort });
test tcFixedLcAbort [main=TestFixedLcAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedLcAbort });
test tcFixedRetry [main=TestFixedRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedRetry });
test tcFixedThenAbort [main=TestFixedThenAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedThenAbort });
test tcFixedPutThenRetry [main=TestFixedPutThenRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedPutThenRetry });
test tcFixedDelVsPut [main=TestFixedDelVsPut]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedDelVsPut });
test tcFixedDelsAndPut [main=TestFixedDelsAndPut]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedDelsAndPut });
test tcFixedDelVsComplete [main=TestFixedDelVsComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedDelVsComplete });
test tcFixedCopyOne [main=TestFixedCopyOne]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedCopyOne });
test tcFixedCopyVsPutSrc [main=TestFixedCopyVsPutSrc]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedCopyVsPutSrc });
test tcFixedCopyVsDelSrc [main=TestFixedCopyVsDelSrc]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedCopyVsDelSrc });
test tcFixedCopyVsPutDst [main=TestFixedCopyVsPutDst]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedCopyVsPutDst });
test tcFixedCopyThenDeletes [main=TestFixedCopyThenDeletes]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedCopyThenDeletes });
test tcFixedCopySelfVsPut [main=TestFixedCopySelfVsPut]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedCopySelfVsPut });
test tcFixedCopyMpu [main=TestFixedCopyMpu]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedCopyMpu });
test tcFixedCrashCopyRetry [main=TestFixedCrashCopyRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedCrashCopyRetry });
test tcFixedListVsPut [main=TestFixedListVsPut]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedListVsPut });
test tcFixedListVsDel [main=TestFixedListVsDel]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedListVsDel });
test tcFixedListVsComplete [main=TestFixedListVsComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedListVsComplete });
test tcFixedDedupThenDeletes [main=TestFixedDedupThenDeletes]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedDedupThenDeletes });
test tcFixedDedupVsPutTgt [main=TestFixedDedupVsPutTgt]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedDedupVsPutTgt });
test tcFixedDedupVsDelTgt [main=TestFixedDedupVsDelTgt]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedDedupVsDelTgt });
test tcFixedDedupVsPutSrc [main=TestFixedDedupVsPutSrc]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedDedupVsPutSrc });
test tcFixedDedupVsCopySelf [main=TestFixedDedupVsCopySelf]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedDedupVsCopySelf });
test tcFixedReshardVsPuts [main=TestFixedReshardVsPuts]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedReshardVsPuts });
test tcFixedReshardVsDel [main=TestFixedReshardVsDel]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedReshardVsDel });
test tcFixedReshardVsMpu [main=TestFixedReshardVsMpu]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedReshardVsMpu });
test tcFixedIxPuts [main=TestFixedIxPuts]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedIxPuts });
test tcFixedIxPutOne [main=TestFixedIxPutOne]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedIxPutOne });
test tcFixedIxPutVsComplete [main=TestFixedIxPutVsComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedIxPutVsComplete });
test tcFixedIxCompletes [main=TestFixedIxCompletes]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedIxCompletes });
test tcFixedIxSameCompletes [main=TestFixedIxSameCompletes]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedIxSameCompletes });
test tcFixedIxReupload [main=TestFixedIxReupload]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedIxReupload });
test tcFixedIxAbort [main=TestFixedIxAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedIxAbort });
test tcFixedIxLcAbort [main=TestFixedIxLcAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedIxLcAbort });
test tcFixedIxRetry [main=TestFixedIxRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedIxRetry });
test tcFixedIxThenAbort [main=TestFixedIxThenAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedIxThenAbort });
test tcFixedIxPutThenRetry [main=TestFixedIxPutThenRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedIxPutThenRetry });
test tcFixedIxDelVsPut [main=TestFixedIxDelVsPut]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedIxDelVsPut });
test tcFixedIxDelsAndPut [main=TestFixedIxDelsAndPut]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedIxDelsAndPut });
test tcFixedIxDelVsComplete [main=TestFixedIxDelVsComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedIxDelVsComplete });
test tcFixedIxCopyOne [main=TestFixedIxCopyOne]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedIxCopyOne });
test tcFixedIxCopyVsPutSrc [main=TestFixedIxCopyVsPutSrc]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedIxCopyVsPutSrc });
test tcFixedIxCopyVsDelSrc [main=TestFixedIxCopyVsDelSrc]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedIxCopyVsDelSrc });
test tcFixedIxCopyVsPutDst [main=TestFixedIxCopyVsPutDst]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedIxCopyVsPutDst });
test tcFixedIxCopyThenDeletes [main=TestFixedIxCopyThenDeletes]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedIxCopyThenDeletes });
test tcFixedIxCopySelfVsPut [main=TestFixedIxCopySelfVsPut]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedIxCopySelfVsPut });
test tcFixedIxCopyMpu [main=TestFixedIxCopyMpu]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedIxCopyMpu });
test tcFixedIxCrashCopyRetry [main=TestFixedIxCrashCopyRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedIxCrashCopyRetry });
test tcFixedIxListVsPut [main=TestFixedIxListVsPut]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedIxListVsPut });
test tcFixedIxListVsDel [main=TestFixedIxListVsDel]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedIxListVsDel });
test tcFixedIxListVsComplete [main=TestFixedIxListVsComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedIxListVsComplete });
test tcFixedIxDedupThenDeletes [main=TestFixedIxDedupThenDeletes]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedIxDedupThenDeletes });
test tcFixedIxDedupVsPutTgt [main=TestFixedIxDedupVsPutTgt]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedIxDedupVsPutTgt });
test tcFixedIxDedupVsDelTgt [main=TestFixedIxDedupVsDelTgt]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedIxDedupVsDelTgt });
test tcFixedIxDedupVsPutSrc [main=TestFixedIxDedupVsPutSrc]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedIxDedupVsPutSrc });
test tcFixedIxDedupVsCopySelf [main=TestFixedIxDedupVsCopySelf]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedIxDedupVsCopySelf });
test tcFixedIxReshardVsPuts [main=TestFixedIxReshardVsPuts]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedIxReshardVsPuts });
test tcFixedIxReshardVsDel [main=TestFixedIxReshardVsDel]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedIxReshardVsDel });
test tcFixedIxReshardVsMpu [main=TestFixedIxReshardVsMpu]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestFixedIxReshardVsMpu });

// proposed fixes for findings 3 and 11
test tcStallListVsPut [main=TestStallListVsPut]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestStallListVsPut });
test tcStallListVsDel [main=TestStallListVsDel]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestStallListVsDel });
test tcStallListVsComplete [main=TestStallListVsComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestStallListVsComplete });
test tcStallListPutDel [main=TestStallListPutDel]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestStallListPutDel });
test tcStallListNewPut [main=TestStallListNewPut]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestStallListNewPut });
test tcRelinkListVsPut [main=TestRelinkListVsPut]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestRelinkListVsPut });
test tcRelinkListVsDel [main=TestRelinkListVsDel]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestRelinkListVsDel });
test tcRelinkListVsComplete [main=TestRelinkListVsComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestRelinkListVsComplete });
test tcRelinkListPutDel [main=TestRelinkListPutDel]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestRelinkListPutDel });
test tcRelinkListNewPut [main=TestRelinkListNewPut]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestRelinkListNewPut });
test tcMarkCrashRetry [main=TestMarkCrashRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestMarkCrashRetry });
test tcMarkCrashThenAbort [main=TestMarkCrashThenAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestMarkCrashThenAbort });
test tcMarkCrashPutThenRetry [main=TestMarkCrashPutThenRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestMarkCrashPutThenRetry });
test tcMarkCrashCrashCopyRetry [main=TestMarkCrashCrashCopyRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestMarkCrashCrashCopyRetry });
test tcMarkCrashSameCompletes [main=TestMarkCrashSameCompletes]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestMarkCrashSameCompletes });
test tcMarkCrashAbort [main=TestMarkCrashAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestMarkCrashAbort });
test tcMarkCrashLcAbort [main=TestMarkCrashLcAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestMarkCrashLcAbort });
test tcMarkMetaDelRetry [main=TestMarkMetaDelRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestMarkMetaDelRetry });
test tcMarkMetaDelThenAbort [main=TestMarkMetaDelThenAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestMarkMetaDelThenAbort });
test tcMarkMetaDelPutThenRetry [main=TestMarkMetaDelPutThenRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestMarkMetaDelPutThenRetry });
test tcMarkMetaDelCrashCopyRetry [main=TestMarkMetaDelCrashCopyRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestMarkMetaDelCrashCopyRetry });
test tcMarkMetaDelSameCompletes [main=TestMarkMetaDelSameCompletes]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestMarkMetaDelSameCompletes });
test tcMarkMetaDelAbort [main=TestMarkMetaDelAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestMarkMetaDelAbort });
test tcMarkMetaDelLcAbort [main=TestMarkMetaDelLcAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestMarkMetaDelLcAbort });
test tcMarkLapseAbort [main=TestMarkLapseAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestMarkLapseAbort });
test tcMarkLapseLcAbort [main=TestMarkLapseLcAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestMarkLapseLcAbort });
test tcMarkLapseSameCompletes [main=TestMarkLapseSameCompletes]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestMarkLapseSameCompletes });
test tcFixedCreates [main=TestFixedCreates]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics in (union System, { TestFixedCreates });
test tcFixedCreateVsComplete [main=TestFixedCreateVsComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics in (union System, { TestFixedCreateVsComplete });
test tcFixedIfMatchVsPut [main=TestFixedIfMatchVsPut]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics in (union System, { TestFixedIfMatchVsPut });
test tcFixedMatchAnyVsMatch [main=TestFixedMatchAnyVsMatch]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics in (union System, { TestFixedMatchAnyVsMatch });
test tcFixedCondDelVsPut [main=TestFixedCondDelVsPut]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics in (union System, { TestFixedCondDelVsPut });
test tcFixedCondDelVsMatch [main=TestFixedCondDelVsMatch]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics in (union System, { TestFixedCondDelVsMatch });
test tcFixedCondCompleteVsPut [main=TestFixedCondCompleteVsPut]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics in (union System, { TestFixedCondCompleteVsPut });
test tcFixedIxCreates [main=TestFixedIxCreates]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics in (union System, { TestFixedIxCreates });
test tcFixedIxCreateVsComplete [main=TestFixedIxCreateVsComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics in (union System, { TestFixedIxCreateVsComplete });
test tcFixedIxIfMatchVsPut [main=TestFixedIxIfMatchVsPut]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics in (union System, { TestFixedIxIfMatchVsPut });
test tcFixedIxMatchAnyVsMatch [main=TestFixedIxMatchAnyVsMatch]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics in (union System, { TestFixedIxMatchAnyVsMatch });
test tcFixedIxCondDelVsPut [main=TestFixedIxCondDelVsPut]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics in (union System, { TestFixedIxCondDelVsPut });
test tcFixedIxCondDelVsMatch [main=TestFixedIxCondDelVsMatch]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics in (union System, { TestFixedIxCondDelVsMatch });
test tcFixedIxCondCompleteVsPut [main=TestFixedIxCondCompleteVsPut]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics in (union System, { TestFixedIxCondCompleteVsPut });

// the fixes together, and the two for conditional requests
test tcCondFixedCreates [main=TestCondFixedCreates]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics in (union System, { TestCondFixedCreates });
test tcCondFixedCreateVsComplete [main=TestCondFixedCreateVsComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics in (union System, { TestCondFixedCreateVsComplete });
test tcCondFixedIfMatchVsPut [main=TestCondFixedIfMatchVsPut]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics in (union System, { TestCondFixedIfMatchVsPut });
test tcCondFixedMatchAnyVsMatch [main=TestCondFixedMatchAnyVsMatch]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics in (union System, { TestCondFixedMatchAnyVsMatch });
test tcCondFixedCondDelVsPut [main=TestCondFixedCondDelVsPut]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics in (union System, { TestCondFixedCondDelVsPut });
test tcCondFixedCondDelVsMatch [main=TestCondFixedCondDelVsMatch]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics in (union System, { TestCondFixedCondDelVsMatch });
test tcCondFixedCondCompleteVsPut [main=TestCondFixedCondCompleteVsPut]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics in (union System, { TestCondFixedCondCompleteVsPut });
test tcCondFixedIxCreates [main=TestCondFixedIxCreates]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics in (union System, { TestCondFixedIxCreates });
test tcCondFixedIxCreateVsComplete [main=TestCondFixedIxCreateVsComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics in (union System, { TestCondFixedIxCreateVsComplete });
test tcCondFixedIxIfMatchVsPut [main=TestCondFixedIxIfMatchVsPut]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics in (union System, { TestCondFixedIxIfMatchVsPut });
test tcCondFixedIxMatchAnyVsMatch [main=TestCondFixedIxMatchAnyVsMatch]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics in (union System, { TestCondFixedIxMatchAnyVsMatch });
test tcCondFixedIxCondDelVsPut [main=TestCondFixedIxCondDelVsPut]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics in (union System, { TestCondFixedIxCondDelVsPut });
test tcCondFixedIxCondDelVsMatch [main=TestCondFixedIxCondDelVsMatch]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics in (union System, { TestCondFixedIxCondDelVsMatch });
test tcCondFixedIxCondCompleteVsPut [main=TestCondFixedIxCondCompleteVsPut]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics in (union System, { TestCondFixedIxCondCompleteVsPut });

// S3 answers, against the Smithy model and the S3 User Guide
test tcAnsPuts [main=TestAnsPuts]:
  assert S3Answers in (union System, { TestAnsPuts });
test tcAnsPutVsComplete [main=TestAnsPutVsComplete]:
  assert S3Answers in (union System, { TestAnsPutVsComplete });
test tcAnsCompletes [main=TestAnsCompletes]:
  assert S3Answers in (union System, { TestAnsCompletes });
test tcAnsSameCompletes [main=TestAnsSameCompletes]:
  assert S3Answers in (union System, { TestAnsSameCompletes });
test tcAnsReupload [main=TestAnsReupload]:
  assert S3Answers in (union System, { TestAnsReupload });
test tcAnsAbort [main=TestAnsAbort]:
  assert S3Answers in (union System, { TestAnsAbort });
test tcAnsLcAbort [main=TestAnsLcAbort]:
  assert S3Answers in (union System, { TestAnsLcAbort });
test tcAnsRetry [main=TestAnsRetry]:
  assert S3Answers in (union System, { TestAnsRetry });
test tcAnsThenAbort [main=TestAnsThenAbort]:
  assert S3Answers in (union System, { TestAnsThenAbort });
test tcAnsPutThenRetry [main=TestAnsPutThenRetry]:
  assert S3Answers in (union System, { TestAnsPutThenRetry });
test tcAnsDelVsPut [main=TestAnsDelVsPut]:
  assert S3Answers in (union System, { TestAnsDelVsPut });
test tcAnsDelsAndPut [main=TestAnsDelsAndPut]:
  assert S3Answers in (union System, { TestAnsDelsAndPut });
test tcAnsDelVsComplete [main=TestAnsDelVsComplete]:
  assert S3Answers in (union System, { TestAnsDelVsComplete });
test tcAnsCopyVsPutSrc [main=TestAnsCopyVsPutSrc]:
  assert S3Answers in (union System, { TestAnsCopyVsPutSrc });
test tcAnsCopyVsDelSrc [main=TestAnsCopyVsDelSrc]:
  assert S3Answers in (union System, { TestAnsCopyVsDelSrc });
test tcAnsCopySelfVsPut [main=TestAnsCopySelfVsPut]:
  assert S3Answers in (union System, { TestAnsCopySelfVsPut });
test tcAnsCopyMpu [main=TestAnsCopyMpu]:
  assert S3Answers in (union System, { TestAnsCopyMpu });
test tcAnsListVsPut [main=TestAnsListVsPut]:
  assert S3Answers in (union System, { TestAnsListVsPut });
test tcAnsCreates [main=TestAnsCreates]:
  assert S3Answers in (union System, { TestAnsCreates });
test tcAnsCreateVsComplete [main=TestAnsCreateVsComplete]:
  assert S3Answers in (union System, { TestAnsCreateVsComplete });
test tcAnsIfMatchVsPut [main=TestAnsIfMatchVsPut]:
  assert S3Answers in (union System, { TestAnsIfMatchVsPut });
test tcAnsMatchAnyVsMatch [main=TestAnsMatchAnyVsMatch]:
  assert S3Answers in (union System, { TestAnsMatchAnyVsMatch });
test tcAnsCondDelVsPut [main=TestAnsCondDelVsPut]:
  assert S3Answers in (union System, { TestAnsCondDelVsPut });
test tcAnsCondDelVsMatch [main=TestAnsCondDelVsMatch]:
  assert S3Answers in (union System, { TestAnsCondDelVsMatch });
test tcAnsCondCompleteVsPut [main=TestAnsCondCompleteVsPut]:
  assert S3Answers in (union System, { TestAnsCondCompleteVsPut });
test tcAnsCondDels [main=TestAnsCondDels]:
  assert S3Answers in (union System, { TestAnsCondDels });
test tcAnsInvalidThenReupload [main=TestAnsInvalidThenReupload]:
  assert S3Answers in (union System, { TestAnsInvalidThenReupload });
test tcAnsIxFailPut [main=TestAnsIxFailPut]:
  assert S3Answers in (union System, { TestAnsIxFailPut });
test tcAnsIxFailRetry [main=TestAnsIxFailRetry]:
  assert S3Answers in (union System, { TestAnsIxFailRetry });
test tcAnsGuardCondDelVsMatch [main=TestAnsGuardCondDelVsMatch]:
  assert S3Answers in (union System, { TestAnsGuardCondDelVsMatch });
test tcAnsTakesLockLcAbort [main=TestAnsTakesLockLcAbort]:
  assert S3Answers in (union System, { TestAnsTakesLockLcAbort });
test tcAnsLossFailsIfMatchVsPut [main=TestAnsLossFailsIfMatchVsPut]:
  assert S3Answers in (union System, { TestAnsLossFailsIfMatchVsPut });
test tcAnsLossFailsCondCompleteVsPut [main=TestAnsLossFailsCondCompleteVsPut]:
  assert S3Answers in (union System, { TestAnsLossFailsCondCompleteVsPut });
test tcAnsNoKeyCondDels [main=TestAnsNoKeyCondDels]:
  assert S3Answers, CondSemantics in (union System, { TestAnsNoKeyCondDels });
test tcAnsHistoryInvalidThenReupload [main=TestAnsHistoryInvalidThenReupload]:
  assert S3Answers, HeadIntact, NoOrphans, IndexMatchesHead, AllAnswered, BucketStats in (union System, { TestAnsHistoryInvalidThenReupload });
test tcAnsFixedPuts [main=TestAnsFixedPuts]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics, S3Answers in (union System, { TestAnsFixedPuts });
test tcAnsFixedPutVsComplete [main=TestAnsFixedPutVsComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics, S3Answers in (union System, { TestAnsFixedPutVsComplete });
test tcAnsFixedCompletes [main=TestAnsFixedCompletes]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics, S3Answers in (union System, { TestAnsFixedCompletes });
test tcAnsFixedSameCompletes [main=TestAnsFixedSameCompletes]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics, S3Answers in (union System, { TestAnsFixedSameCompletes });
test tcAnsFixedReupload [main=TestAnsFixedReupload]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics, S3Answers in (union System, { TestAnsFixedReupload });
test tcAnsFixedAbort [main=TestAnsFixedAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics, S3Answers in (union System, { TestAnsFixedAbort });
test tcAnsFixedLcAbort [main=TestAnsFixedLcAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics, S3Answers in (union System, { TestAnsFixedLcAbort });
test tcAnsFixedRetry [main=TestAnsFixedRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics, S3Answers in (union System, { TestAnsFixedRetry });
test tcAnsFixedThenAbort [main=TestAnsFixedThenAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics, S3Answers in (union System, { TestAnsFixedThenAbort });
test tcAnsFixedPutThenRetry [main=TestAnsFixedPutThenRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics, S3Answers in (union System, { TestAnsFixedPutThenRetry });
test tcAnsFixedDelVsPut [main=TestAnsFixedDelVsPut]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics, S3Answers in (union System, { TestAnsFixedDelVsPut });
test tcAnsFixedDelsAndPut [main=TestAnsFixedDelsAndPut]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics, S3Answers in (union System, { TestAnsFixedDelsAndPut });
test tcAnsFixedDelVsComplete [main=TestAnsFixedDelVsComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics, S3Answers in (union System, { TestAnsFixedDelVsComplete });
test tcAnsFixedCopyVsDelSrc [main=TestAnsFixedCopyVsDelSrc]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics, S3Answers in (union System, { TestAnsFixedCopyVsDelSrc });
test tcAnsFixedCopyMpu [main=TestAnsFixedCopyMpu]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics, S3Answers in (union System, { TestAnsFixedCopyMpu });
test tcAnsFixedReshardVsMpu [main=TestAnsFixedReshardVsMpu]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics, S3Answers in (union System, { TestAnsFixedReshardVsMpu });
test tcAnsFixedCreates [main=TestAnsFixedCreates]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics, S3Answers in (union System, { TestAnsFixedCreates });
test tcAnsFixedCreateVsComplete [main=TestAnsFixedCreateVsComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics, S3Answers in (union System, { TestAnsFixedCreateVsComplete });
test tcAnsFixedIfMatchVsPut [main=TestAnsFixedIfMatchVsPut]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics, S3Answers in (union System, { TestAnsFixedIfMatchVsPut });
test tcAnsFixedMatchAnyVsMatch [main=TestAnsFixedMatchAnyVsMatch]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics, S3Answers in (union System, { TestAnsFixedMatchAnyVsMatch });
test tcAnsFixedCondDelVsPut [main=TestAnsFixedCondDelVsPut]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics, S3Answers in (union System, { TestAnsFixedCondDelVsPut });
test tcAnsFixedCondDelVsMatch [main=TestAnsFixedCondDelVsMatch]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics, S3Answers in (union System, { TestAnsFixedCondDelVsMatch });
test tcAnsFixedCondCompleteVsPut [main=TestAnsFixedCondCompleteVsPut]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics, S3Answers in (union System, { TestAnsFixedCondCompleteVsPut });
test tcAnsFixedCondDels [main=TestAnsFixedCondDels]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics, S3Answers in (union System, { TestAnsFixedCondDels });
test tcAnsFixedInvalidThenReupload [main=TestAnsFixedInvalidThenReupload]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics, S3Answers in (union System, { TestAnsFixedInvalidThenReupload });
test tcAnsFixedIxPutVsComplete [main=TestAnsFixedIxPutVsComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics, S3Answers in (union System, { TestAnsFixedIxPutVsComplete });
test tcAnsFixedIxCompletes [main=TestAnsFixedIxCompletes]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics, S3Answers in (union System, { TestAnsFixedIxCompletes });
test tcAnsFixedIxSameCompletes [main=TestAnsFixedIxSameCompletes]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics, S3Answers in (union System, { TestAnsFixedIxSameCompletes });
test tcAnsFixedIxReupload [main=TestAnsFixedIxReupload]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics, S3Answers in (union System, { TestAnsFixedIxReupload });
test tcAnsFixedIxRetry [main=TestAnsFixedIxRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics, S3Answers in (union System, { TestAnsFixedIxRetry });
test tcAnsFixedIxCreateVsComplete [main=TestAnsFixedIxCreateVsComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics, S3Answers in (union System, { TestAnsFixedIxCreateVsComplete });
test tcAnsFixedIxCondCompleteVsPut [main=TestAnsFixedIxCondCompleteVsPut]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics, S3Answers in (union System, { TestAnsFixedIxCondCompleteVsPut });
test tcAnsFixedIxCondDels [main=TestAnsFixedIxCondDels]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics, S3Answers in (union System, { TestAnsFixedIxCondDels });
test tcAnsFixedIxInvalidThenReupload [main=TestAnsFixedIxInvalidThenReupload]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics, S3Answers in (union System, { TestAnsFixedIxInvalidThenReupload });
test tcAnsFixedMarkCrashRetry [main=TestAnsFixedMarkCrashRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics, S3Answers in (union System, { TestAnsFixedMarkCrashRetry });
test tcAnsFixedMarkCrashThenAbort [main=TestAnsFixedMarkCrashThenAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics, S3Answers in (union System, { TestAnsFixedMarkCrashThenAbort });
test tcAnsFixedMarkCrashInvalidThenReupload [main=TestAnsFixedMarkCrashInvalidThenReupload]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats, CondSemantics, S3Answers in (union System, { TestAnsFixedMarkCrashInvalidThenReupload });

// versioned buckets
test tcVEPuts [main=TestVEPuts]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVEPuts });
test tcVEDelVsPut [main=TestVEDelVsPut]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVEDelVsPut });
test tcVERetry [main=TestVERetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVERetry });
test tcVEDelAfterRetry [main=TestVEDelAfterRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVEDelAfterRetry });
test tcVEPutThenAbort [main=TestVEPutThenAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVEPutThenAbort });
test tcVSPuts [main=TestVSPuts]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSPuts });
test tcVSDelVsPut [main=TestVSDelVsPut]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSDelVsPut });
test tcVSRetry [main=TestVSRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSRetry });
test tcVSDelAfterRetry [main=TestVSDelAfterRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSDelAfterRetry });
test tcVSPutThenAbort [main=TestVSPutThenAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSPutThenAbort });
test tcMarkCrashDelAfterRetry [main=TestMarkCrashDelAfterRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestMarkCrashDelAfterRetry });
test tcMarkCrashPutThenAbort [main=TestMarkCrashPutThenAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestMarkCrashPutThenAbort });
test tcMarkMetaDelDelAfterRetry [main=TestMarkMetaDelDelAfterRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestMarkMetaDelDelAfterRetry });
test tcMarkMetaDelPutThenAbort [main=TestMarkMetaDelPutThenAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestMarkMetaDelPutThenAbort });
test tcVEPrCrashRetry [main=TestVEPrCrashRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVEPrCrashRetry });
test tcVEPrCrashThenAbort [main=TestVEPrCrashThenAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVEPrCrashThenAbort });
test tcVEPrCrashPutThenRetry [main=TestVEPrCrashPutThenRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVEPrCrashPutThenRetry });
test tcVEPrCrashSameCompletes [main=TestVEPrCrashSameCompletes]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVEPrCrashSameCompletes });
test tcVEPrCrashAbort [main=TestVEPrCrashAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVEPrCrashAbort });
test tcVEPrCrashLcAbort [main=TestVEPrCrashLcAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVEPrCrashLcAbort });
test tcVEPrCrashDelAfterRetry [main=TestVEPrCrashDelAfterRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVEPrCrashDelAfterRetry });
test tcVEPrCrashPutThenAbort [main=TestVEPrCrashPutThenAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVEPrCrashPutThenAbort });
test tcVEPrMetaDelRetry [main=TestVEPrMetaDelRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVEPrMetaDelRetry });
test tcVEPrMetaDelThenAbort [main=TestVEPrMetaDelThenAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVEPrMetaDelThenAbort });
test tcVEPrMetaDelPutThenRetry [main=TestVEPrMetaDelPutThenRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVEPrMetaDelPutThenRetry });
test tcVEPrMetaDelSameCompletes [main=TestVEPrMetaDelSameCompletes]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVEPrMetaDelSameCompletes });
test tcVEPrMetaDelAbort [main=TestVEPrMetaDelAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVEPrMetaDelAbort });
test tcVEPrMetaDelLcAbort [main=TestVEPrMetaDelLcAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVEPrMetaDelLcAbort });
test tcVEPrMetaDelDelAfterRetry [main=TestVEPrMetaDelDelAfterRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVEPrMetaDelDelAfterRetry });
test tcVEPrMetaDelPutThenAbort [main=TestVEPrMetaDelPutThenAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVEPrMetaDelPutThenAbort });
test tcVECurCrashRetry [main=TestVECurCrashRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVECurCrashRetry });
test tcVECurCrashThenAbort [main=TestVECurCrashThenAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVECurCrashThenAbort });
test tcVECurCrashPutThenRetry [main=TestVECurCrashPutThenRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVECurCrashPutThenRetry });
test tcVECurCrashSameCompletes [main=TestVECurCrashSameCompletes]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVECurCrashSameCompletes });
test tcVECurCrashAbort [main=TestVECurCrashAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVECurCrashAbort });
test tcVECurCrashLcAbort [main=TestVECurCrashLcAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVECurCrashLcAbort });
test tcVECurCrashDelAfterRetry [main=TestVECurCrashDelAfterRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVECurCrashDelAfterRetry });
test tcVECurCrashPutThenAbort [main=TestVECurCrashPutThenAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVECurCrashPutThenAbort });
test tcVECurMetaDelRetry [main=TestVECurMetaDelRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVECurMetaDelRetry });
test tcVECurMetaDelThenAbort [main=TestVECurMetaDelThenAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVECurMetaDelThenAbort });
test tcVECurMetaDelPutThenRetry [main=TestVECurMetaDelPutThenRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVECurMetaDelPutThenRetry });
test tcVECurMetaDelSameCompletes [main=TestVECurMetaDelSameCompletes]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVECurMetaDelSameCompletes });
test tcVECurMetaDelAbort [main=TestVECurMetaDelAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVECurMetaDelAbort });
test tcVECurMetaDelLcAbort [main=TestVECurMetaDelLcAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVECurMetaDelLcAbort });
test tcVECurMetaDelDelAfterRetry [main=TestVECurMetaDelDelAfterRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVECurMetaDelDelAfterRetry });
test tcVECurMetaDelPutThenAbort [main=TestVECurMetaDelPutThenAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVECurMetaDelPutThenAbort });
test tcVEInstCrashRetry [main=TestVEInstCrashRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVEInstCrashRetry });
test tcVEInstCrashThenAbort [main=TestVEInstCrashThenAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVEInstCrashThenAbort });
test tcVEInstCrashPutThenRetry [main=TestVEInstCrashPutThenRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVEInstCrashPutThenRetry });
test tcVEInstCrashSameCompletes [main=TestVEInstCrashSameCompletes]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVEInstCrashSameCompletes });
test tcVEInstCrashAbort [main=TestVEInstCrashAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVEInstCrashAbort });
test tcVEInstCrashLcAbort [main=TestVEInstCrashLcAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVEInstCrashLcAbort });
test tcVEInstCrashDelAfterRetry [main=TestVEInstCrashDelAfterRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVEInstCrashDelAfterRetry });
test tcVEInstCrashPutThenAbort [main=TestVEInstCrashPutThenAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVEInstCrashPutThenAbort });
test tcVEInstMetaDelRetry [main=TestVEInstMetaDelRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVEInstMetaDelRetry });
test tcVEInstMetaDelThenAbort [main=TestVEInstMetaDelThenAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVEInstMetaDelThenAbort });
test tcVEInstMetaDelPutThenRetry [main=TestVEInstMetaDelPutThenRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVEInstMetaDelPutThenRetry });
test tcVEInstMetaDelSameCompletes [main=TestVEInstMetaDelSameCompletes]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVEInstMetaDelSameCompletes });
test tcVEInstMetaDelAbort [main=TestVEInstMetaDelAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVEInstMetaDelAbort });
test tcVEInstMetaDelLcAbort [main=TestVEInstMetaDelLcAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVEInstMetaDelLcAbort });
test tcVEInstMetaDelDelAfterRetry [main=TestVEInstMetaDelDelAfterRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVEInstMetaDelDelAfterRetry });
test tcVEInstMetaDelPutThenAbort [main=TestVEInstMetaDelPutThenAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVEInstMetaDelPutThenAbort });
test tcVSPrCrashRetry [main=TestVSPrCrashRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSPrCrashRetry });
test tcVSPrCrashThenAbort [main=TestVSPrCrashThenAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSPrCrashThenAbort });
test tcVSPrCrashPutThenRetry [main=TestVSPrCrashPutThenRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSPrCrashPutThenRetry });
test tcVSPrCrashSameCompletes [main=TestVSPrCrashSameCompletes]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSPrCrashSameCompletes });
test tcVSPrCrashAbort [main=TestVSPrCrashAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSPrCrashAbort });
test tcVSPrCrashLcAbort [main=TestVSPrCrashLcAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSPrCrashLcAbort });
test tcVSPrCrashDelAfterRetry [main=TestVSPrCrashDelAfterRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSPrCrashDelAfterRetry });
test tcVSPrCrashPutThenAbort [main=TestVSPrCrashPutThenAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSPrCrashPutThenAbort });
test tcVSPrMetaDelRetry [main=TestVSPrMetaDelRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSPrMetaDelRetry });
test tcVSPrMetaDelThenAbort [main=TestVSPrMetaDelThenAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSPrMetaDelThenAbort });
test tcVSPrMetaDelPutThenRetry [main=TestVSPrMetaDelPutThenRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSPrMetaDelPutThenRetry });
test tcVSPrMetaDelSameCompletes [main=TestVSPrMetaDelSameCompletes]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSPrMetaDelSameCompletes });
test tcVSPrMetaDelAbort [main=TestVSPrMetaDelAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSPrMetaDelAbort });
test tcVSPrMetaDelLcAbort [main=TestVSPrMetaDelLcAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSPrMetaDelLcAbort });
test tcVSPrMetaDelDelAfterRetry [main=TestVSPrMetaDelDelAfterRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSPrMetaDelDelAfterRetry });
test tcVSPrMetaDelPutThenAbort [main=TestVSPrMetaDelPutThenAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSPrMetaDelPutThenAbort });
test tcVSCurCrashRetry [main=TestVSCurCrashRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSCurCrashRetry });
test tcVSCurCrashThenAbort [main=TestVSCurCrashThenAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSCurCrashThenAbort });
test tcVSCurCrashPutThenRetry [main=TestVSCurCrashPutThenRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSCurCrashPutThenRetry });
test tcVSCurCrashSameCompletes [main=TestVSCurCrashSameCompletes]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSCurCrashSameCompletes });
test tcVSCurCrashAbort [main=TestVSCurCrashAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSCurCrashAbort });
test tcVSCurCrashLcAbort [main=TestVSCurCrashLcAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSCurCrashLcAbort });
test tcVSCurCrashDelAfterRetry [main=TestVSCurCrashDelAfterRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSCurCrashDelAfterRetry });
test tcVSCurCrashPutThenAbort [main=TestVSCurCrashPutThenAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSCurCrashPutThenAbort });
test tcVSCurMetaDelRetry [main=TestVSCurMetaDelRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSCurMetaDelRetry });
test tcVSCurMetaDelThenAbort [main=TestVSCurMetaDelThenAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSCurMetaDelThenAbort });
test tcVSCurMetaDelPutThenRetry [main=TestVSCurMetaDelPutThenRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSCurMetaDelPutThenRetry });
test tcVSCurMetaDelSameCompletes [main=TestVSCurMetaDelSameCompletes]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSCurMetaDelSameCompletes });
test tcVSCurMetaDelAbort [main=TestVSCurMetaDelAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSCurMetaDelAbort });
test tcVSCurMetaDelLcAbort [main=TestVSCurMetaDelLcAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSCurMetaDelLcAbort });
test tcVSCurMetaDelDelAfterRetry [main=TestVSCurMetaDelDelAfterRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSCurMetaDelDelAfterRetry });
test tcVSCurMetaDelPutThenAbort [main=TestVSCurMetaDelPutThenAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSCurMetaDelPutThenAbort });
test tcVSInstCrashRetry [main=TestVSInstCrashRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSInstCrashRetry });
test tcVSInstCrashThenAbort [main=TestVSInstCrashThenAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSInstCrashThenAbort });
test tcVSInstCrashPutThenRetry [main=TestVSInstCrashPutThenRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSInstCrashPutThenRetry });
test tcVSInstCrashSameCompletes [main=TestVSInstCrashSameCompletes]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSInstCrashSameCompletes });
test tcVSInstCrashAbort [main=TestVSInstCrashAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSInstCrashAbort });
test tcVSInstCrashLcAbort [main=TestVSInstCrashLcAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSInstCrashLcAbort });
test tcVSInstCrashDelAfterRetry [main=TestVSInstCrashDelAfterRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSInstCrashDelAfterRetry });
test tcVSInstCrashPutThenAbort [main=TestVSInstCrashPutThenAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSInstCrashPutThenAbort });
test tcVSInstMetaDelRetry [main=TestVSInstMetaDelRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSInstMetaDelRetry });
test tcVSInstMetaDelThenAbort [main=TestVSInstMetaDelThenAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSInstMetaDelThenAbort });
test tcVSInstMetaDelPutThenRetry [main=TestVSInstMetaDelPutThenRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSInstMetaDelPutThenRetry });
test tcVSInstMetaDelSameCompletes [main=TestVSInstMetaDelSameCompletes]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSInstMetaDelSameCompletes });
test tcVSInstMetaDelAbort [main=TestVSInstMetaDelAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSInstMetaDelAbort });
test tcVSInstMetaDelLcAbort [main=TestVSInstMetaDelLcAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSInstMetaDelLcAbort });
test tcVSInstMetaDelDelAfterRetry [main=TestVSInstMetaDelDelAfterRetry]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSInstMetaDelDelAfterRetry });
test tcVSInstMetaDelPutThenAbort [main=TestVSInstMetaDelPutThenAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestVSInstMetaDelPutThenAbort });

// sharding
test tcShH1H2ObjVsPuts [main=TestShH1H2ObjVsPuts]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH1H2ObjVsPuts });
test tcShH1H2ObjVsDel [main=TestShH1H2ObjVsDel]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH1H2ObjVsDel });
test tcShH1H2ObjVsMpu [main=TestShH1H2ObjVsMpu]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH1H2ObjVsMpu });
test tcShH1H2ObjThenComplete [main=TestShH1H2ObjThenComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH1H2ObjThenComplete });
test tcShH1H2ObjThenAbort [main=TestShH1H2ObjThenAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH1H2ObjThenAbort });
test tcShH1H2ObjVEVsPuts [main=TestShH1H2ObjVEVsPuts]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH1H2ObjVEVsPuts });
test tcShH1H2ObjVEThenComplete [main=TestShH1H2ObjVEThenComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH1H2ObjVEThenComplete });
test tcShH1H2ObjVSVsPuts [main=TestShH1H2ObjVSVsPuts]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH1H2ObjVSVsPuts });
test tcShH1H2ObjVSThenComplete [main=TestShH1H2ObjVSThenComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH1H2ObjVSThenComplete });
test tcShH1H2IdxVsPuts [main=TestShH1H2IdxVsPuts]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH1H2IdxVsPuts });
test tcShH1H2IdxVsDel [main=TestShH1H2IdxVsDel]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH1H2IdxVsDel });
test tcShH1H2IdxVsMpu [main=TestShH1H2IdxVsMpu]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH1H2IdxVsMpu });
test tcShH1H2IdxThenComplete [main=TestShH1H2IdxThenComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH1H2IdxThenComplete });
test tcShH1H2IdxThenAbort [main=TestShH1H2IdxThenAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH1H2IdxThenAbort });
test tcShH1H2IdxVEVsPuts [main=TestShH1H2IdxVEVsPuts]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH1H2IdxVEVsPuts });
test tcShH1H2IdxVEThenComplete [main=TestShH1H2IdxVEThenComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH1H2IdxVEThenComplete });
test tcShH1H2IdxVSVsPuts [main=TestShH1H2IdxVSVsPuts]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH1H2IdxVSVsPuts });
test tcShH1H2IdxVSThenComplete [main=TestShH1H2IdxVSThenComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH1H2IdxVSThenComplete });
test tcShH2H2ObjVsPuts [main=TestShH2H2ObjVsPuts]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH2H2ObjVsPuts });
test tcShH2H2ObjVsDel [main=TestShH2H2ObjVsDel]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH2H2ObjVsDel });
test tcShH2H2ObjVsMpu [main=TestShH2H2ObjVsMpu]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH2H2ObjVsMpu });
test tcShH2H2ObjThenComplete [main=TestShH2H2ObjThenComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH2H2ObjThenComplete });
test tcShH2H2ObjThenAbort [main=TestShH2H2ObjThenAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH2H2ObjThenAbort });
test tcShH2H2ObjVEVsPuts [main=TestShH2H2ObjVEVsPuts]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH2H2ObjVEVsPuts });
test tcShH2H2ObjVEThenComplete [main=TestShH2H2ObjVEThenComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH2H2ObjVEThenComplete });
test tcShH2H2ObjVSVsPuts [main=TestShH2H2ObjVSVsPuts]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH2H2ObjVSVsPuts });
test tcShH2H2ObjVSThenComplete [main=TestShH2H2ObjVSThenComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH2H2ObjVSThenComplete });
test tcShH2H2IdxVsPuts [main=TestShH2H2IdxVsPuts]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH2H2IdxVsPuts });
test tcShH2H2IdxVsDel [main=TestShH2H2IdxVsDel]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH2H2IdxVsDel });
test tcShH2H2IdxVsMpu [main=TestShH2H2IdxVsMpu]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH2H2IdxVsMpu });
test tcShH2H2IdxThenComplete [main=TestShH2H2IdxThenComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH2H2IdxThenComplete });
test tcShH2H2IdxThenAbort [main=TestShH2H2IdxThenAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH2H2IdxThenAbort });
test tcShH2H2IdxVEVsPuts [main=TestShH2H2IdxVEVsPuts]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH2H2IdxVEVsPuts });
test tcShH2H2IdxVEThenComplete [main=TestShH2H2IdxVEThenComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH2H2IdxVEThenComplete });
test tcShH2H2IdxVSVsPuts [main=TestShH2H2IdxVSVsPuts]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH2H2IdxVSVsPuts });
test tcShH2H2IdxVSThenComplete [main=TestShH2H2IdxVSThenComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH2H2IdxVSThenComplete });
test tcShH2O2ObjVsPuts [main=TestShH2O2ObjVsPuts]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH2O2ObjVsPuts });
test tcShH2O2ObjVsDel [main=TestShH2O2ObjVsDel]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH2O2ObjVsDel });
test tcShH2O2ObjVsMpu [main=TestShH2O2ObjVsMpu]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH2O2ObjVsMpu });
test tcShH2O2ObjThenComplete [main=TestShH2O2ObjThenComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH2O2ObjThenComplete });
test tcShH2O2ObjThenAbort [main=TestShH2O2ObjThenAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH2O2ObjThenAbort });
test tcShH2O2ObjVEVsPuts [main=TestShH2O2ObjVEVsPuts]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH2O2ObjVEVsPuts });
test tcShH2O2ObjVEThenComplete [main=TestShH2O2ObjVEThenComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH2O2ObjVEThenComplete });
test tcShH2O2ObjVSVsPuts [main=TestShH2O2ObjVSVsPuts]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH2O2ObjVSVsPuts });
test tcShH2O2ObjVSThenComplete [main=TestShH2O2ObjVSThenComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH2O2ObjVSThenComplete });
test tcShH2O2IdxVsPuts [main=TestShH2O2IdxVsPuts]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH2O2IdxVsPuts });
test tcShH2O2IdxVsDel [main=TestShH2O2IdxVsDel]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH2O2IdxVsDel });
test tcShH2O2IdxVsMpu [main=TestShH2O2IdxVsMpu]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH2O2IdxVsMpu });
test tcShH2O2IdxThenComplete [main=TestShH2O2IdxThenComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH2O2IdxThenComplete });
test tcShH2O2IdxThenAbort [main=TestShH2O2IdxThenAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH2O2IdxThenAbort });
test tcShH2O2IdxVEVsPuts [main=TestShH2O2IdxVEVsPuts]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH2O2IdxVEVsPuts });
test tcShH2O2IdxVEThenComplete [main=TestShH2O2IdxVEThenComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH2O2IdxVEThenComplete });
test tcShH2O2IdxVSVsPuts [main=TestShH2O2IdxVSVsPuts]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH2O2IdxVSVsPuts });
test tcShH2O2IdxVSThenComplete [main=TestShH2O2IdxVSThenComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH2O2IdxVSThenComplete });
test tcShO2H2ObjVsPuts [main=TestShO2H2ObjVsPuts]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShO2H2ObjVsPuts });
test tcShO2H2ObjVsDel [main=TestShO2H2ObjVsDel]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShO2H2ObjVsDel });
test tcShO2H2ObjVsMpu [main=TestShO2H2ObjVsMpu]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShO2H2ObjVsMpu });
test tcShO2H2ObjThenComplete [main=TestShO2H2ObjThenComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShO2H2ObjThenComplete });
test tcShO2H2ObjThenAbort [main=TestShO2H2ObjThenAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShO2H2ObjThenAbort });
test tcShO2H2ObjVEVsPuts [main=TestShO2H2ObjVEVsPuts]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShO2H2ObjVEVsPuts });
test tcShO2H2ObjVEThenComplete [main=TestShO2H2ObjVEThenComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShO2H2ObjVEThenComplete });
test tcShO2H2ObjVSVsPuts [main=TestShO2H2ObjVSVsPuts]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShO2H2ObjVSVsPuts });
test tcShO2H2ObjVSThenComplete [main=TestShO2H2ObjVSThenComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShO2H2ObjVSThenComplete });
test tcShO2H2IdxVsPuts [main=TestShO2H2IdxVsPuts]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShO2H2IdxVsPuts });
test tcShO2H2IdxVsDel [main=TestShO2H2IdxVsDel]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShO2H2IdxVsDel });
test tcShO2H2IdxVsMpu [main=TestShO2H2IdxVsMpu]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShO2H2IdxVsMpu });
test tcShO2H2IdxThenComplete [main=TestShO2H2IdxThenComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShO2H2IdxThenComplete });
test tcShO2H2IdxThenAbort [main=TestShO2H2IdxThenAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShO2H2IdxThenAbort });
test tcShO2H2IdxVEVsPuts [main=TestShO2H2IdxVEVsPuts]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShO2H2IdxVEVsPuts });
test tcShO2H2IdxVEThenComplete [main=TestShO2H2IdxVEThenComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShO2H2IdxVEThenComplete });
test tcShO2H2IdxVSVsPuts [main=TestShO2H2IdxVSVsPuts]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShO2H2IdxVSVsPuts });
test tcShO2H2IdxVSThenComplete [main=TestShO2H2IdxVSThenComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShO2H2IdxVSThenComplete });
test tcShH1O2ObjVsPuts [main=TestShH1O2ObjVsPuts]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH1O2ObjVsPuts });
test tcShH1O2ObjVsDel [main=TestShH1O2ObjVsDel]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH1O2ObjVsDel });
test tcShH1O2ObjVsMpu [main=TestShH1O2ObjVsMpu]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH1O2ObjVsMpu });
test tcShH1O2ObjThenComplete [main=TestShH1O2ObjThenComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH1O2ObjThenComplete });
test tcShH1O2ObjThenAbort [main=TestShH1O2ObjThenAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH1O2ObjThenAbort });
test tcShH1O2ObjVEVsPuts [main=TestShH1O2ObjVEVsPuts]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH1O2ObjVEVsPuts });
test tcShH1O2ObjVEThenComplete [main=TestShH1O2ObjVEThenComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH1O2ObjVEThenComplete });
test tcShH1O2ObjVSVsPuts [main=TestShH1O2ObjVSVsPuts]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH1O2ObjVSVsPuts });
test tcShH1O2ObjVSThenComplete [main=TestShH1O2ObjVSThenComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH1O2ObjVSThenComplete });
test tcShH1O2IdxVsPuts [main=TestShH1O2IdxVsPuts]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH1O2IdxVsPuts });
test tcShH1O2IdxVsDel [main=TestShH1O2IdxVsDel]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH1O2IdxVsDel });
test tcShH1O2IdxVsMpu [main=TestShH1O2IdxVsMpu]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH1O2IdxVsMpu });
test tcShH1O2IdxThenComplete [main=TestShH1O2IdxThenComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH1O2IdxThenComplete });
test tcShH1O2IdxThenAbort [main=TestShH1O2IdxThenAbort]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH1O2IdxThenAbort });
test tcShH1O2IdxVEVsPuts [main=TestShH1O2IdxVEVsPuts]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH1O2IdxVEVsPuts });
test tcShH1O2IdxVEThenComplete [main=TestShH1O2IdxVEThenComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH1O2IdxVEThenComplete });
test tcShH1O2IdxVSVsPuts [main=TestShH1O2IdxVSVsPuts]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH1O2IdxVSVsPuts });
test tcShH1O2IdxVSThenComplete [main=TestShH1O2IdxVSThenComplete]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH1O2IdxVSThenComplete });
test tcShGuardH1H2ObjVsDel [main=TestShH1H2ObjVsDelGuard]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, CompletionEtag, AllAnswered, BucketStats in (union System, { TestShH1H2ObjVsDelGuard });

// attribute updates
test tcAttrMainVsTag [main=TestAttrMainVsTag]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, AttrsKept, AllAnswered, BucketStats in (union System, { TestAttrMainVsTag });
test tcAttrMainTagVsPut [main=TestAttrMainTagVsPut]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, AttrsKept, AllAnswered, BucketStats in (union System, { TestAttrMainTagVsPut });
test tcAttrMainTagThenCopy [main=TestAttrMainTagThenCopy]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, AttrsKept, AllAnswered, BucketStats in (union System, { TestAttrMainTagThenCopy });
test tcAttrMainVsPut [main=TestAttrMainVsPut]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, AttrsKept, AllAnswered, BucketStats in (union System, { TestAttrMainVsPut });
test tcAttrGuardVsTag [main=TestAttrGuardVsTag]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, AttrsKept, AllAnswered, BucketStats in (union System, { TestAttrGuardVsTag });
test tcAttrGuardTagVsPut [main=TestAttrGuardTagVsPut]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, AttrsKept, AllAnswered, BucketStats in (union System, { TestAttrGuardTagVsPut });
test tcAttrGuardTagThenCopy [main=TestAttrGuardTagThenCopy]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, AttrsKept, AllAnswered, BucketStats in (union System, { TestAttrGuardTagThenCopy });
test tcAttrGuardVsPut [main=TestAttrGuardVsPut]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, AttrsKept, AllAnswered, BucketStats in (union System, { TestAttrGuardVsPut });
test tcAttrRetryVsTag [main=TestAttrRetryVsTag]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, AttrsKept, AllAnswered, BucketStats in (union System, { TestAttrRetryVsTag });
test tcAttrRetryTagVsPut [main=TestAttrRetryTagVsPut]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, AttrsKept, AllAnswered, BucketStats in (union System, { TestAttrRetryTagVsPut });
test tcAttrRetryTagThenCopy [main=TestAttrRetryTagThenCopy]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, AttrsKept, AllAnswered, BucketStats in (union System, { TestAttrRetryTagThenCopy });
test tcAttrRetryVsPut [main=TestAttrRetryVsPut]:
  assert HeadIntact, NoOrphans, IndexMatchesHead, AttrsKept, AllAnswered, BucketStats in (union System, { TestAttrRetryVsPut });
