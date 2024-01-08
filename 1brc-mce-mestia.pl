#!/usr/bin/perl
use strict;
use warnings;
use feature 'say';
use MCE;
use MCE::Loop;

usage{
    die "$0 <filename> <numbers of forks>\n";
}

usage() if ! $ARGV[0];

my $file = shift // die "Usage: $0 filename\n";
my $proc                = shift // 8;
my $data                 = {};
my $size                 = -s $file;

my $chunk_size = int ($size/$proc);

MCE::Loop->init(
    max_workers => $proc,
    chunk_size => $chunk_size,
    use_slurpio => 1,
    parallel_io => 1,
);

my @results =  mce_loop_f {
    my ($mce, $chunk_ref, $chunk_id) = @_;
    MCE->gather(proc_chunk($_));
} $file;

MCE::Loop->finish;

update_global_hash($_) for @results;

print "{";
for ( sort keys %$data ) {
    my $cd = $data->{$_};
    printf "%s=%.1f/%.1f/%.1f, ", $_,$cd->{min} , $cd->{sum}/$cd->{cnt}, $cd->{max};
}
say "}\n";

sub proc_chunk {
    my $data = {};
    for my $line ( (split('\n',${$_[0]}))) {
        my ( $city, $temp ) = split( ';', $line );    # get city and temperature
        if ( $data->{$city} ) {
            my $cd = $data->{$city};
            if ( $temp > $cd->{max} ) {    # max
                $cd->{max} = $temp;
            }
            elsif ( $temp < $cd->{min} ) {    # min
                $cd->{min} = $temp;
            }
            $cd->{sum} += $temp;
            $cd->{cnt}++;
        }
        else {
            $data->{$city} = { max => $temp, min => $temp, sum => $temp,
                cnt => 1 }                    # initialise city
        }
    }
    return $data;
}

sub update_global_hash {
    my ($datast) = @_;
    for my $city ( keys %{$datast} ) {
        my $max = $datast->{$city}->{max};
        my $min = $datast->{$city}->{min};
        my $sum = $datast->{$city}->{sum};
        my $cnt = $datast->{$city}->{cnt};
        if ( $data->{$city} ) {
            my $cd = $data->{$city};
            if ( $max > $cd->{max} ) {    # max
                $cd->{max} = $max;
            }
            elsif ( $min < $cd->{min} ) {    # min
                $cd->{min} = $min;
            }
            $cd->{sum} += $sum;
            $cd->{cnt} += $cnt;
        }
        else {
            $data->{$city} = $datast->{$city};    #init
        }
    }
}
