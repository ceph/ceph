An Quantitative Analysis to Ceph Data Reliability

Author: Li Wang <laurence.liwang@gmail.com>

Ceph relies on data redundancy mechanism to enhence data reliability. The data is stored in
multi-copies. If one disk encounters failure, the data recovery mechanism is triggered to
maintain the configured number of copies. Suppose the recovery time is t, the replica count
is r, then only if during the time interval t, there are k>=r disk failures, and the k disks
constitute at least a PG, it will cause data loss. Let¡¯s calculate the probability of data loss.

First, let us calculate the probability of r disk failure in time t.

In the theory of probability and statistics, a Bernoulli trial[1] is a random experiment with
exactly two possible outcomes, 'success' and 'failure', in which the probability of success
is the same every time the experiment conducted. A binomial experiment, which consists of a
fixed number n of statistically independent Bernoulli trials, each with a probability of
success p, and counts the number of successes. A random variable corresponding to a binomial
experiment is denoted by B(n,p) , and is said to have a binomial distribution[2].

In time t, whether a disk encounters failure can be treated as a Bernoulli trial, and the
probability of r disk failures among total n disks have a binomial distribution. Thus
the probability of r disk failures among n disks in time t is

B(n,r,p) = C(n,r)*p^r*(1-p)^(n-r)

while C(n,r) is 'n choose r' in the field of combinatorics.

When n is large, and p is very small, B(n,r,p) is not easy to calculate. If n is
sufficiently large and p is sufficiently small, the Poisson distribution with parameter
lamda = np can be used as an approximation to B(n,p) of the binomial distribution[3].
This approximation is good if n ¡Ý 20 and p ¡Ü 0.05. According to the Poisson distribution,

B(n,r,p) = lamda^r*e^(-lamda)/r!

Suppose the MTBF(Mean Time Between Failures) of a disk is m, then

p = t/m
lamda = np = nt/m
B(n,r,p) = (nt/m)^r*e(-nt/m)/r!

Now, we use an concrete example, suppose m=10000 hours, t=1 hour, n=200, r=3, then

p = 1/10000 = 1*10e-4
lamda = 200*1*10e-4 = 2*10e-2
B(200,3,1*10e-4) = 1.3*10-6

This is the probability of 3 disk failures among 200 disks in 1 hour.

Next, we calculate the probability of these failure disks constituting a PG. This is dependent
on the CRUSH rule, PG number, and the cluster topology. Let us continue to use a concrete
example, suppose there are total 20 machines with 10 disks each, the osds have one-to-one mapping
to the disks, and the CRUSH chooses one osd for a machine. Then the theoretical maximal number
of PGs (each has a different osd set) is C(20,3)*C(10,1)*C(10,1)*C(10,1) = 1140000,
while the number of total osd set (with 3 osds) is C(200,3) = 1313400, suppose the configured
PG number is 4096, then the probability of 3 disks consitituting a PG is

P_3_pg = min(4096,1140000)/1314000

now we can calculate the probability of data loss when 3 disk failures in 1 hour as follows,

P_3_loss = B(200,3,p)*P_3_pg = 4*10e-9

The probability of data loss is

P_loss = P_3_loss+P_4_loss+...+P_200_loss

However, since P_4_loss is smaller than 1% of P_3_loss, P_4_loss and later could be ignored,
and use P_3_loss to approximate P_loss

Then the data reliability is 1-4*10e-9 = 0.999999996

As a result, the data reliability of Ceph is dependent on the following factors,

Total disk number n
Recovery time t
Replica count r
Disk MTBF m
CRUSH rule
PG number

especially, n, t, r have relatively large impact

Note: This analysis applies only to replica configuration, the erasure code configuration
will be added later

Reference

[1] https://en.wikipedia.org/wiki/Bernoulli_trial
[2] https://en.wikipedia.org/wiki/Binomial_distribution
[3] https://en.wikipedia.org/wiki/Poisson_distribution
