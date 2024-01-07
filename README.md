# 1brc-perl
Another solutions for 1 Billion Record Challenge in Perl

1brc-perl-semipar.pl reads file and forks when number of lines is reached

1brc-perl-parallel.pl splits file into chunks and forks code for each section - higher memory usage
but can utilize more cpu cores, bottleneck is obviously the I/O.
