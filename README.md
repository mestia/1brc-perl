### Solutions for [1 Billion Record Challenge in Perl](https://www.morling.dev/blog/one-billion-row-challenge/)

* 1brc-perl-semipar.pl reads file and forks when number of lines is reached, uses [Parallel::ForkManager](https://metacpan.org/pod/Parallel::ForkManager)

* 1brc-pfm-mestia.pl splits file into chunks and forks code for each section - higher memory usage but can utilize more cpu cores, bottleneck is obviously the I/O.
Has a problem when requesting more cores than available, buffer passed to proc_chunc() gets rundomly truncated.

* 1brc-mce-mestia.pl [MCE](https://metacpan.org/pod/MCE) implementation

### Source file
compile and run create-sample.c for the sample file. Code is available here:
https://github.com/dannyvankooten/1brc

### Some stats ( running on AMD EPYC 7702P 64-Core Processor and a SSD pool )

```
 64 cores; real: 0m23.951s, user: 23m4.662s,  sys: 0m13.939s ; time perl 1brc-mce-mestia.pl ./measurements.txt 64
 8 cores ; real: 1m59.429s, user: 15m36.224s, sys: 0m7.145s  ; time perl 1brc-mce-mestia.pl ./measurements.txt 8
 8 cores ; real: 2m0.195s,  user: 15m22.971s, sys: 0m27.302s ; time perl 1brc-pfm-mestia.pl ./measurements.txt 8

```

Probably the cleanest solution without modules like P::FM or MCE, https://github.com/adriaandens/1brc.pl/blob/main/forkie.pl

another MCE option: https://github.com/janlimpens/1brc-perl/blob/main/calculate_average_mce.pl


See also:

* [Reddit](https://www.reddit.com/r/perl/comments/18ygpsi/1_billion_row_challenge_in_perl/)
* [Show and Tell](https://github.com/gunnarmorling/1brc/discussions/categories/show-and-tell)
