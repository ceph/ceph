# Credits

This test suite is a Rust translation of the
[Ceph s3-tests](https://github.com/ceph/s3-tests) Python test suite, with
additional tests ported from the
[bucket notification test suite](https://github.com/ceph/ceph/tree/main/src/test/rgw/bucket_notification)
(`test_bn.py`).

The translation would not exist without the substantial work of the original
authors. The contributors below are listed in descending order by commit count,
with a brief characterization of the areas their work covered.

## Python s3-tests

| Commits | Contributor | Areas |
|--------:|-------------|-------|
| 234 | Casey Bodley | core S3 ops, IAM, headers, test infrastructure |
| 216 | Yehuda Sadeh | core S3 ops, test framework, bootstrap |
| 87 | Tommi Virtanen | core S3 ops, realistic workloads, config |
| 73 | Ali Maredia | S3 ops, headers, fuzzer, realistic workloads |
| 52 | Stephon Striplin | S3 ops, headers |
| 50 | Gal Salomon | S3 Select tests |
| 49 | Yuval Lifshitz | S3 ops, STS, config |
| 48 | Andrew Gaul | S3 ops, headers |
| 41 | Abhishek Lekshmanan | S3 ops, policy |
| 38 | Matt Benjamin | S3 ops, Rust test conversion |
| 37 | Soumya Koduri | S3 ops, headers, STS, config |
| 33 | Robin H. Johnson | S3 website tests, config |
| 32 | Kyle Marsh | fuzzer, object generation |
| 23 | Steven Berler | realistic workloads, S3 ops |
| 20 | Wesley Spikes | realistic workloads, greenlets |
| 20 | Seena Fallah | S3 ops, policy |
| 14 | Zhang Shaowen | S3 ops |
| 13 | Pritha Srivastava | STS tests, S3 ops |
| 13 | Alfredo Deza | realistic workloads |
| 11 | Orit Wasserman | S3 ops, bootstrap |
| 11 | Javier M. Mellid | S3 ops, headers |
| 9 | Albin Antony | S3 Select tests |
| 8 | Joe Buck | readwrite workloads, S3 ops |
| 6 | Raja Sharma | S3 ops, IAM, STS |
| 6 | N Balachandran | S3 ops |
| 6 | Jane Zhu | S3 ops |
| 6 | Ali Masarwa | S3 ops, SNS |
| 6 | Adam Kupczyk | S3 ops |
| 5 | Or Friedmann | S3 ops |
| 5 | iraj465 | S3 ops |
| 5 | caleb miles | S3 ops |
| 4 | yuliyang | S3 ops |
| 4 | Josh Durgin | bootstrap |
| 4 | Jiffin Tony Thottan | S3 ops, config |
| 4 | Colin Patrick McCabe | S3 ops |
| 4 | Adam C. Emerson | S3 ops, utilities |
| 3 | Tobias Urdin | S3 ops |
| 3 | Tianshan Qu | S3 ops, roundtrip |
| 3 | Sage Weil | S3 ops, headers |
| 3 | Matthew Wodrich | S3 ops, object generation |
| 3 | Kefu Chai | bootstrap, packaging |
| 3 | hechuang | S3 ops |
| 3 | Emin | S3 ops |
| 2 | Vasu Kulkarni | bootstrap |
| 2 | tobe | documentation |
| 2 | Tim Burke | S3 ops |
| 2 | sungjoon_koh | S3 ops |
| 2 | Ray Lv | S3 ops |
| 2 | Pragadeeswaran Sathyanarayanan | S3 ops, config |
| 2 | Oguzhan Ozmen | S3 ops |
| 2 | Mark Kogan | S3 ops |
| 2 | Mark Kampe | S3 ops, headers, fuzzer |
| 2 | Ken Dreyer | bootstrap |
| 2 | Kalpesh Pandya | STS tests, config |
| 2 | Justin Restivo | documentation |
| 2 | Evgenii Gorinov | S3 ops |
| 2 | Enming Zhang | S3 ops |
| 2 | Daniel Gryniewicz | S3 ops |
| 2 | Andrea Baglioni | S3 ops |
| 2 | albIN7 | S3 ops |
| 1 | 胡玮文 | S3 ops |
| 1 | Zuhair AlSader | bootstrap |
| 1 | Yuan Zhou | roundtrip, packaging |
| 1 | Xinying Song | S3 ops |
| 1 | Wyllys Ingersoll | headers |
| 1 | Wido den Hollander | S3 ops |
| 1 | Timur Alperovich | S3 ops |
| 1 | Sumedh A. Kulkarni | S3 ops |
| 1 | Shriya Deshmukh | S3 ops |
| 1 | shreyanshjain7174 | S3 ops, config |
| 1 | Sandon Van Ness | bootstrap |
| 1 | Romy | S3 ops |
| 1 | Ravindra Choudhari | IAM, documentation |
| 1 | Rahul Dev Parashar | S3 ops |
| 1 | Priya Sehgal | S3 ops |
| 1 | Peter Ginchev | S3 ops |
| 1 | Pei | config |
| 1 | Nithya Balachandran | S3 ops |
| 1 | nadav mizrahi | documentation |
| 1 | Moritz Röhrich | packaging |
| 1 | Mark Houghton | S3 ops |
| 1 | Marcus Watts | bootstrap |
| 1 | Malcolm Lee | S3 ops |
| 1 | Luis Pabón | S3 ops |
| 1 | Liu Lan | documentation |
| 1 | liranmauda | bootstrap |
| 1 | Kyr Shatskyy | S3 ops |
| 1 | Kyle Bader | S3 ops |
| 1 | Ilsoo Byun | S3 website tests |
| 1 | Igor Fedotov | S3 ops |
| 1 | fang yuxiang | S3 ops |
| 1 | Danny Abukalam | S3 ops |
| 1 | Dan Mick | bootstrap |
| 1 | Cory Snyder | S3 ops |
| 1 | chang liu | S3 ops |
| 1 | Andrew Schoen | S3 ops |
| 1 | Amit Prinz Setter | S3 Select tests |

## Bucket notification tests (test_bn.py)

| Commits | Contributor | Areas |
|--------:|-------------|-------|
| 69 | Yuval Lifshitz | primary author; Kafka, AMQP, HTTP notification delivery |
| 21 | Ali Masarwa | notification tests, API |
| 10 | Casey Bodley | notification tests |
| 8 | Krunal Chheda | notification tests, API |
| 6 | Kalpesh Pandya | notification tests |
| 4 | Oguzhan Ozmen | notification tests |
| 3 | XueYu Bai | notification tests, documentation |
| 2 | Tom Schoonjans | notification tests |
| 2 | rkhudov | documentation |
| 2 | Matt Benjamin | notification tests, Rust test conversion |
| 1 | 邓伟键 | notification tests |
| 1 | Zhipeng Li | notification tests |
| 1 | sujay-d07 | notification tests, documentation |
| 1 | Pritha Srivastava | notification tests |
| 1 | Oshrey Avraham | notification tests, API |
| 1 | Josh Soref | notification tests, API |
| 1 | Jane Zhu | notification tests |
| 1 | Jan Radon | notification tests, Kafka security |
| 1 | igomon | notification tests |
| 1 | Kefu Chai | bootstrap |
| 1 | Hoai-Thu Vuong | notification tests, packaging |
| 1 | Arjun Sharma | notification tests |
| 1 | Adarsh | notification tests, API |
| 1 | Patrick Donnelly | infrastructure |
| 1 | J. Eric Ivancich | infrastructure |
