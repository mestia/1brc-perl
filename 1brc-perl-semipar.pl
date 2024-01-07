use warnings;
use strict;
use feature qw(say state);
use Parallel::ForkManager;

#use Data::Dumper;
#$Data::Dumper::Indent   = 1;
#$Data::Dumper::Sortkeys = 1;

my $file = shift // die "Usage: $0 filename\n";

my $pm = Parallel::ForkManager->new(8);

open my $fh, '<', $file or die $!;

my $num_lines;
my @arr;
my $data     = {};
my $uniqcity = {};
while ( my $line = <$fh> ) {
    chomp $line;

    push @arr, $line;

    # fork every 31250000 lines proc_chunk()
    if ( ++$num_lines >= 31250000 ) {    #1000000000/32
        $pm->run_on_finish(
            sub {
                my ( $pid, $exit_code, $ident, $exit_signal, $core_dump,
                    $datast )
                  = @_;
                update_global_hash($datast);
            }
        );
        $pm->start and do {
            $num_lines = 0;
            @arr       = ();
            next;
        };
        my $ret = proc_chunk( \@arr );
        $pm->finish( 0, $ret );
    }
}
$pm->wait_all_children;

if (@arr) {
    my $rest = proc_chunk( \@arr );
    update_global_hash($rest);
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

# code inspired by https://github.com/gustafe/1brc-perl/blob/main/baseline.pl
sub proc_chunk {
    my $data = {};
    for my $line ( @{ $_[0] } ) {
        my ( $city, $temp ) = split( ';', $line );    # get city and temperature
              #$temp=~s/\.//;                           # remove decimal point
        $temp *= 10;    # remove decimal point
        use integer;    # speeds up by 10%
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

print "{";
for ( sort keys %$data ) {                    # print results
    my $cd = $data->{$_};
    print $_, ";", $cd->{min} / 10, "/",
      ( int( $cd->{sum} / $cd->{cnt} + 5 ) / 10 ), "/", $cd->{max} / 10, ", ";
}
say "}\n";
