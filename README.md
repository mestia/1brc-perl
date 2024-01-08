# 1brc-perl
Solutions for 1 Billion Record Challenge in Perl

1brc-perl-semipar.pl reads file and forks when number of lines is reached, uses [Parallel::ForkManager](https://metacpan.org/pod/Parallel::ForkManager)

1brc-pfm-mestia.pl splits file into chunks and forks code for each section - higher memory usage but can utilize more cpu cores, bottleneck is obviously the I/O.
Has a problem when requesting more cores than available, buffer passed to proc_chunc() gets rundomly truncated.

1brc-mce-mestia.pl [MCE](https://metacpan.org/pod/MCE) implementation

compile and run create-sample.c for the sample file. Code is available here:
https://github.com/dannyvankooten/1brc


Some stats:

```
 64 Cores of AMD EPYC 7702P 64-Core Processor and SSD pool time perl ./1brc-mce-mestia.pl ./measurements.txt 64 real: 0m24.709s user:22m18.649s sys:3m6.488s
 8  Cores of AMD EPYC 7702P 64-Core Processor and SSD pool time perl ./1brc-mce-mestia.pl ./measurements.txt 8 real:  3m43.824s user:15m22.695s sys:12m45.656s

```
