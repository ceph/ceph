===========
PGBackend.h
===========

Work in progress:
::
 
 /**
  * PGBackend
  *
  * PGBackend defines an interface for logic handling IO and
  * replication on RADOS objects.  The PGBackend implementation
  * is responsible for:
  *
  * 1) Handling client operations
  * 2) Handling object recovery
  * 3) Handling object access
  */
 class PGBackend {
 public:	
   /// IO
 
   /// Perform write
   int perform_write(
     const vector<OSDOp> &ops,  ///< [in] ops to perform
     Context *onreadable,       ///< [in] called when readable on all reaplicas
     Context *onreadable,       ///< [in] called when durable on all replicas
     ) = 0; ///< @return 0 or error
 
   /// Attempt to roll back a log entry
   int try_rollback(
     const pg_log_entry_t &entry, ///< [in] entry to roll back
     ObjectStore::Transaction *t  ///< [out] transaction
     ) = 0; ///< @return 0 on success, -EINVAL if it can't be rolled back
 
   /// Perform async read, oncomplete is called when ops out_bls are filled in
   int perform_read(
     vector<OSDOp> &ops,        ///< [in, out] ops
     Context *oncomplete        ///< [out] called with r code
     ) = 0; ///< @return 0 or error
 
   /// Peering
 
   /**
    * have_enough_infos
    *
    * Allows PGBackend implementation to ensure that enough peers have
    * been contacted to satisfy its requirements.
    *
    * TODO: this interface should yield diagnostic info about which infos
    * are required
    */
   bool have_enough_infos(
     const map<epoch_t, pg_interval_t> &past_intervals,      ///< [in] intervals
     const map<chunk_id_t, map<int, pg_info_t> > &peer_infos ///< [in] infos
     ) = 0; ///< @return true if we can continue peering
 
   /**
    * choose_acting
    *
    * Allows PGBackend implementation to select the acting set based on the
    * received infos
    *
    * @return False if the current acting set is inadequate, *req_acting will
    *         be filled in with the requested new acting set.  True if the
    *         current acting set is adequate, *auth_log will be filled in
    *         with the correct location of the authoritative log.
    */
   bool choose_acting(
     const map<int, pg_info_t> &peer_infos, ///< [in] received infos
     int *auth_log,                         ///< [out] osd with auth log
     vector<int> *req_acting                ///< [out] requested acting set
     ) = 0;
 
   /// Scrub
 
   /// scan
   int scan(
     const hobject_t &start, ///< [in] scan objects >= start
     const hobject_t &up_to, ///< [in] scan objects < up_to
     vector<hobject_t> *out  ///< [out] objects returned
     ) = 0; ///< @return 0 or error
 
   /// stat (TODO: ScrubMap::object needs to have PGBackend specific metadata)
   int scrub(
     const hobject_t &to_stat, ///< [in] object to stat
     bool deep,                ///< [in] true if deep scrub
     ScrubMap::object *o       ///< [out] result
     ) = 0; ///< @return 0 or error
 
   /**
    * compare_scrub_maps
    *
    * @param inconsistent [out] map of inconsistent pgs to pair<correct, incorrect>
    * @param errstr [out] stream of text about inconsistencies for user
    *                     perusal
    *
    * TODO: this interface doesn't actually make sense...
    */
   void compare_scrub_maps(
     const map<int, ScrubMap> &maps, ///< [in] maps to compare
     bool deep,                      ///< [in] true if scrub is deep
     map<hobject_t, pair<set<int>, set<int> > > *inconsistent,
     std:ostream *errstr
     ) = 0;
 
   /// Recovery
 
   /**
    * might_have_unrecoverable
    *
    * @param missing [in] missing,info gathered so far (must include acting)
    * @param intervals [in] past intervals
    * @param should_query [out] pair<int, cpg_t> shards to query
    */
   void might_have_unrecoverable(
     const map<chunk_id_t, map<int, pair<pg_info_t, pg_missing_t> > &missing,
     const map<epoch_t, pg_interval_t> &past_intervals,
     set<pair<int, cpg_t> > *should_query
     ) = 0;
 
   /**
    * might_have_unfound
    *
    * @param missing [in] missing,info gathered so far (must include acting)
    */
   bool recoverable(
     const map<chunk_id_t, map<int, pair<pg_info_t, pg_missing_t> > &missing,
     const hobject_t &hoid ///< [in] object to check
     ) = 0; ///< @return true if object can be recovered given missing
 
   /**
    * recover_object
    *
    * Triggers a recovery operation on the specified hobject_t
    * onreadable must be called before onwriteable
    *
    * @param missing [in] set of info, missing pairs for queried nodes
    */
   void recover_object(
     const hobject_t &hoid, ///< [in] object to recover
     const map<chunk_id_t, map<int, pair<pg_info_t, pg_missing_t> > &missing
     Context *onreadable,   ///< [in] called when object can be read
     Context *onwriteable   ///< [in] called when object can be written
     ) = 0;
 
   /// Backfill
 
   /// choose_backfill
   void choose_backfill(
     const map<chunk_id_t, map<int, pg_info_t> > &peer_infos ///< [in] infos
     const vector<int> &acting, ///< [in] acting set
     const vector<int> &up,     ///< [in] up set
     set<int> *to_backfill      ///< [out] osds to backfill
     ) = 0;
 };
