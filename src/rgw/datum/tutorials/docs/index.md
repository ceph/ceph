<div class="datum-intro" markdown="1">

<p class="datum-eyebrow">Adventures for Data-Wranglers!</p>

# Datum

Datum is a powerful foundation for building clear, composable data-handling code in Ceph.
It provides shared operations for recurring jobs: gathering fragmented payloads, selecting byte
ranges, ordering records, and choosing fields for encoding or diagnostics.

Its standard-library core brings together byte geometry, interval queries, record projections,
comparisons, and traversal. These operations compose naturally with C++ Ranges, while domain
vocabularies connect them to native Ceph buffers and wire formats.

Gateway services, object classes, and storage integrations can build on the same vocabulary and
processing logic, reducing the separate helpers each component needs to maintain. Views,
projections, and explicit ownership help components pass data between operations and avoid
unnecessary copies while retaining control of I/O, transactions, and storage policy.

</div>

<!-- datum:read-plan -->

## One selection, two ways

With `<array>` and `<ceph-datum.h>` included, and `namespace datum = ceph::salad::datum;`,
select bytes from an ordinary Ceph `buffer::list`:

```cpp
ceph::buffer::list stored;
stored.append(ceph::buffer::copy("Miso: m", 7));
stored.append(ceph::buffer::copy("eow", 3));

const datum::byte_extent meow {6, 4};
std::array<char, 4> output {};
datum::copy(stored, meow, output);
const ceph::buffer::list retained = datum::slice(stored, meow);
```

Both operations select `"meow"` across the two buffers. `copy` writes into the array;
`slice` returns another Ceph list sharing ownership of the selected payloads, so those bytes
remain alive even after `stored` is cleared. An ordinary `std::span` would borrow storage
without keeping it alive.

## What can you build?

- **Bytes and buffers:** [read fragmented payloads](tutorial/01-hello-fragments.md), select byte ranges,
  and retain storage without first assembling a contiguous buffer.
- **Records and codecs:** [project record fields](tutorial/08-toy-basket.md) and
  [encode selected fields](tutorial/12-postcard.md) using shared record declarations.
- **Keys and ordering:** [build ordered keys](tutorial/04-numeric-keys.md) and compose comparisons
  for standard Ranges algorithms, from a single field to a compound ordering.
- **Traversal and budgets:** [continue across pages](tutorial/14-turn-page.md) and
  [account for work](tutorial/13-three-tickets.md) while the application controls fetching and scheduling.

<div class="datum-paths">
  <a class="datum-card datum-card-domain" href="tutorial/18-ceph-bytes/">
    <span class="datum-card-kicker">Working with Ceph</span>
    <strong>Domain Vocabulary: Ceph <span aria-hidden="true">→</span></strong>
    <span>Map object positions into buffer storage, copy a selection, and retain native slices
      with the shared ceph-datum.h header.</span>
  </a>
  <a class="datum-card" href="tutorial/">
    <span class="datum-card-kicker">Begin here</span>
    <strong>Tutorial <span aria-hidden="true">→</span></strong>
    <span>Start with hello, world! in pieces. Follow the lessons at your own pace.</span>
  </a>
  <a class="datum-card" href="design/">
    <span class="datum-card-kicker">Understand the choices</span>
    <strong>Design <span aria-hidden="true">→</span></strong>
    <span>Read about the layers, ownership rules, and boundaries behind the interfaces.</span>
  </a>
  <a class="datum-card" href="build-and-run/">
    <span class="datum-card-kicker">Try it yourself</span>
    <strong>Build and run <span aria-hidden="true">→</span></strong>
    <span>Set up the toolchain, compile a lesson, and check its output.</span>
  </a>
  <a class="datum-card" href="examples/">
    <span class="datum-card-kicker">Keep the code close</span>
    <strong>Complete examples <span aria-hidden="true">→</span></strong>
    <span>Read each runnable program and its variation, or download the sources.</span>
  </a>
</div>

Read the short [developer’s guide](developer-guide.md) for practical design principles,
ownership, progress, errors, and a comparison of related programming approaches.

Find a task or look up a term in the [index](lookup.md), or explore the
[annotated references](references.md).

<div class="datum-chapters" markdown="1">

## In the tutorial

<!-- datum:chapters -->

</div>
