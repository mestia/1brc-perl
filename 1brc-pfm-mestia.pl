#!/usr/bin/perl
# split file into <forks> buffers and run calculations in parallel
# heavily depends on I/O, faster disks - more CPUs can be utilized
# 3.14 minutes with 8 cores and ssd drives
# 1.52 minutes with 16 cores on the same hardware
# a bug, when requested more cores than available the buffer
# sometimes gets truncated in the middle of a line
use strict;
use warnings;
use feature 'say';
use Fcntl qw(SEEK_SET SEEK_CUR SEEK_END);    # better than using 0, 1, 2
use Parallel::ForkManager;
#use Data::Dumper;
#$Data::Dumper::Indent   = 1;
#$Data::Dumper::Sortkeys = 1;

usage{
    die "$0 <filename> <numbers of forks>\n";
}

usage() if ! $ARGV[0];

my $file = shift // die "Usage: $0 filename\n";
my $forks                = shift // 8;
my $data                 = {};
my $start                = 0;
my $size                 = -s $file;
my $approx_buffer_length = $size / $forks;

open my $fh, '<:mmap', $file or die "Cant open file $file, $!";
my $bend = $approx_buffer_length;

my $pm      = Parallel::ForkManager->new( $forks + 1 );
my @offsets = ();
while ( $bend < $size ) {
    last if $bend == $size;
    $bend = getboundaries( $fh, $start, $approx_buffer_length, $size );
    push @offsets, [ $start, $bend ];
    $start = $bend;
}

for my $chunk (@offsets) {
    my ( $start, $end ) = @$chunk;

    $pm->run_on_finish(
        sub {
            my ( $pid, $exit_code, $ident, $exit_signal, $core_dump, $datast )
              = @_;
            update_global_hash($datast);
        }
    );
    $pm->start and next;
    my $ret = proc_chunk( $fh, $start, $end - $start );
    $pm->finish( 0, $ret );
}
$pm->wait_all_children;

print "{";
for ( sort keys %$data ) {    # print results
    my $cd = $data->{$_};
    printf "%s=%.1f/%.1f/%.1f, ", $_,$cd->{min} , $cd->{sum}/$cd->{cnt}, $cd->{max};
}
say "}\n";

sub proc_chunk {
    my ( $fh, $start, $length ) = @_;
    seek $fh, $start, SEEK_SET;
    my $buffer;
    read $fh, $buffer, $length;
    my @buf = split( '\n', $buffer );
    $buffer = '';

    my $data = {};
    for my $line (@buf) {
        my ( $city, $temp ) = split( ';', $line );    # get city and temperature
        if ( $data->{$city} ) {
            my $cd = $data->{$city}
              ;    # create a local copy to speed up access for calculations
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

sub getboundaries {
    my ( $fh, $start, $length, $size ) = @_;
    return $size if $start + $length > $size;
    seek $fh, $start + $length, SEEK_SET;
    my $line = <$fh>;
    $bend = tell $fh;
    return $bend;
}
