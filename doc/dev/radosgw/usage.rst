============================
Usage Design Overview
============================




Testing
-------

The current usage testing does the following:

Following these operations:

 - Create a few buckets
 - Remove buckets
 - Create a bucket
 - Put object
 - Remove object

Test:

1. Verify that 'usage show' with delete_obj category isn't empty after no more than 45 seconds (wait to flush)
2. Check the following

  - 'usage show'

    - does not error out
    - num of entries > 0
    - num of summary entries > 0
    - for every entry in categories check successful_ops > 0
    - check that correct uid in the user summary


  - 'usage show' with specified uid (--uid=<uid>')

    - num of entries > 0
    - num of summary entries > 0
    - for every entry in categories check successful_ops > 0
    - check that correct uid in the user summary

  - 'usage show' with specified uid and specified categories (create_bucket,
    put_obj, delete_obj, delete_bucket)

    - for each category:
      - does not error out
      - num of entries > 0
      - user in user summary is correct user
      - length of categories entries under user summary is exactly 1
      - name of category under user summary is correct name
      - successful ops for the category > 0

  - 'usage trim' with specified uid
    - does not error
    - check following 'usage show' shows complete usage info cleared for user


Additional required testing:

 - test multiple users

   Do the same as in (2), with multiple users being set up.

 - test with multiple buckets (> 1000 * factor, e.g., 2000)

   Create multiple buckets, put objects in each. Account the number written data and verify
   that usage reports show the expected number (up to a certain delta).

 - verify usage show with a date/time range

   Take timestamp of the beginning of the test, and the end of the test. Round timestamps to the
   nearest hour (downward from start of test, upward from the end of test). List data starting
   at end-time, make sure that no data is being shown. List data ending at start-time, make sure
   that no data is shown. List data beginning at start-time, make sure that correct data is
   displayed. List data ending end end-time, make sure that correct data is displayed. List
   data beginning in begin-time, ending in end-time, make sure that correct data is displayed.

 - verify usage trim with a date/time range

   Take timestamp of the beginning of the test, and the end of the test. Round timestamps to the
   nearest hour (downward from start of test, upward from the end of test). Trim data starting
   at end-time, make sure that no data has been trimmed. Trim data ending at start-time, make sure
   that no data has been trimmed.  Trim data beginning in begin-time, ending in end-time, make sure
   that all data has been trimmed.
