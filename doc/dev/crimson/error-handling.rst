==============
error handling
==============


In Seastar, a ``future`` represents a value not yet available but that can become
available later. ``future`` can have one of following states:

* unavailable: value is not available yet,
* value,
* failed: an exception was thrown when computing the value. This exception has
  been captured and stored in the ``future`` instance via ``std::exception_ptr``.

In the last case, the exception can be processed using ``future::handle_exception()`` or
``future::handle_exception_type()``. Seastar even provides ``future::or_terminate()`` to
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

Unfortunately, ``seastar::future`` is not able to satisfy these two requirements.

* Seastar imposes re-throwing an exception to dispatch between different types of
  exceptions. This is not very performant nor even scalable as locking in the language's
  runtime can occur.
* Seastar does not encode the expected exception type in the type of the returned
  ``seastar::future``. Only the type of the value is encoded. This imposes huge
  mental load on programmers as ensuring that all intended errors are indeed handled
  requires manual code audit.

.. highlight:: c++

So, "errorator" is created. It is a wrapper around the vanilla ``seastar::future``.
It addresses the performance and scalability issues while embedding the information
about all expected types-of-errors to the type-of-future.::

  using ertr = crimson::errorator<crimson::ct_error::enoent,
                                  crimson::ct_error::einval>;

In above example we defined an errorator that allows for two error types:

* ``crimson::ct_error::enoent`` and
* ``crimson::ct_error::einval``.

These (and other ones in the ``crimson::ct_error`` namespace) are basically
unthrowable wrappers over ``std::error_code`` to exclude accidental throwing
and ensure signaling errors in a way that enables compile-time checking.

The most fundamental thing in an errorator is a descendant of ``seastar::future``
which can be used as e.g. function's return type::

  static ertr::future<int> foo(int bar) {
    if (bar == 42) {
      return crimson::ct_error::einval::make();
    } else {
      return ertr::make_ready_future(bar);
    }
  }

It's worth to note that returning an error that is not a part the errorator's error set
would result in a compile-time error::

  static ertr::future<int> foo(int bar) {
    // Oops, input_output_error is not allowed in `ertr`.  static_assert() will
    // terminate the compilation. This behaviour is absolutely fundamental for
    // callers -- to figure out about all possible errors they need to worry
    // about is enough to just take a look on the function's signature; reading
    // through its implementation is not necessary anymore!
    return crimson::ct_error::input_output_error::make();
  }

The errorator concept goes further. It not only provides callers with the information
about all potential errors embedded in the function's type; it also ensures at the caller
site that all these errors are handled. As the reader probably know, the main method
in ``seastar::future`` is ``then()``. On errorated future it is available but only if errorator's
error set is empty (literally: ``errorator<>::future``); otherwise callers have
to use ``safe_then()`` instead::

  seastar::future<> baz() {
    return foo(42).safe_then(
      [] (const int bar) {
        std::cout << "the optimistic path! got bar=" << bar << std::endl
        return ertr::now();
      },
      ertr::all_same_way(const std::error_code& err) {
        // handling errors removes them from errorator's error set
        std::cout << "the error path! got err=" << err << std::endl;
        return ertr::now();
      }).then([] {
        // as all errors have been handled, errorator's error set became
        // empty and the future instance returned from `safe_then()` has
        // `then()` available!
        return seastar::now();
      });
  }

In the above example ``ertr::all_same_way`` has been used to handle all errors in the same
manner. This is not obligatory -- a caller can handle each of them separately. Moreover,
it can provide a handler for only a subset of errors. The price for that is the availability
of ``then()``::

  using einval_ertr = crimson::errorator<crimson::ct_error::einval>;

  // we can't return seastar::future<> (aka errorator<>::future<>) as handling
  // as this level deals only with enoent leaving einval without a handler.
  // handling it becomes a responsibility of a caller of `baz()`.
  einval_ertr::future<> baz() {
    return foo(42).safe_then(
      [] (const int bar) {
        std::cout << "the optimistic path! got bar=" << bar << std::endl
        return ertr::now();
      },
      // provide a handler only for crimson::ct_error::enoent.
      // crimson::ct_error::einval stays unhandled!
      crimson::ct_error::enoent::handle([] {
        std::cout << "the enoent error path!" << std::endl;
        return ertr::now();
      }));
    // .safe_then() above returned `errorator<crimson::ct_error::einval>::future<>`
    // which lacks `then()`.
  }

That is, handling errors removes them from errorated future's error set. This works
in the opposite direction too -- returning new errors in ``safe_then()`` appends them
the error set. Of course, this set must be compliant with error set in the ``baz()``'s
signature::

  using broader_ertr = crimson::errorator<crimson::ct_error::enoent,
                                          crimson::ct_error::einval,
                                          crimson::ct_error::input_output_error>;

  broader_ertr::future<> baz() {
    return foo(42).safe_then(
      [] (const int bar) {
        std::cout << "oops, the optimistic path generates a new error!";
        return crimson::ct_error::input_output_error::make();
      },
      // we have a special handler to delegate the handling up. For convenience,
      // the same behaviour is available as single argument-taking variant of
      // `safe_then()`.
      ertr::pass_further{});
  }

As it can be seen, handling and signaling errors in ``safe_then()`` is basically
an operation on the error set checked at compile-time.

More details can be found in `the slides from ceph::errorator<> throw/catch-free,
compile time-checked exceptions for seastar::future<>
<https://www.slideshare.net/ScyllaDB/cepherrorator-throwcatchfree-compile-timechecked-exceptions-for-seastarfuture>`_
presented at the Seastar Summit 2019.
