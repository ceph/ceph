==============
error handling
==============


In Seastar, a `future` represents a value not yet available. It can have one of
following states

* unavailable
* value
* failed: an exception is thrown when computing the value

In the last case, the exception can be processed using `future::handle_exception()` or
`future::handle_exception_type()`. Seastar even provides `future::or_terminate()` to
terminate the program if the future fails.

But in Crimson, quite a few errors are not serious enough to fail the program entirely.
For instance, if we try to look up an object by its object id, and that operation could
fail because the object does not exist or it is corrupted, we need to recover that object
for fulfilling the request instead of terminating the process.

In other words, these errors are expected. Moreover, the performance of the unhappy path
should also be on par with that of the happy path. Also, we want to have a way to ensure
that all expected errors are handled. It should be something like the statical analysis
performed by compiler to spit a warning if any enum value is not handled in a ``switch-case``
statement.

But `seastar::future` is not able to fulfill these two requirement.

* Seastar dispatches the error handling routine using the runtime ``type_info`` of the
  exception. So it is not very performant.
* Seastar does not encode the expected exception type in the type of the returned
  `seastar::future`. Only the type of the value is encoded.

So, "errorator" is created. It is a wrapper around the vanilla `seastar::future`.
