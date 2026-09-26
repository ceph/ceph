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
