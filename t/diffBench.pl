#!/usr/bin/perl

use warnings;
use strict;
use File::Slurp qw/read_file/;
use Term::ANSIColor;

my $f1 = _parse(read_file($ARGV[0]));
my $f2 = _parse(read_file($ARGV[1]));
_diff($f1, $f2);

sub _diff {
    my($e1, $e2) = @_;
    my $longestname = 0;
    for my $name (sort keys %{$e1}) {
        if(defined $e2->{$name}) {
            $name =~ s/^Benchmark//gmx;
            $name =~ s/-\d+$//gmx;
            if(length($name) > $longestname) { $longestname = length($name); }
        }
    }
    printf("%-".$longestname."s       %10s   /     %s\n", "", $ARGV[0], $ARGV[1]);
    for my $name (sort keys %{$e1}) {
        if(defined $e2->{$name}) {
            my $r1 = $e1->{$name};
            my $r2 = $e2->{$name};
            # name
            $name =~ s/^Benchmark//gmx;
            $name =~ s/-\d+$//gmx;
            printf("%-".$longestname."s  ", $name);
            # duration
            printf("%10s / %-10s ns/op %s", $r1->[2], $r2->[2], _color(($r2->[2] / $r1->[2]) * 100));
            # bytes
            printf("  |   %10s / %-10s B/op %s", $r1->[3], $r2->[3], _color(($r2->[3] / $r1->[3]) * 100));
            # allocs
            printf("  |   %7s / %-7s allocs/op %s", $r1->[4], $r2->[4], _color(($r2->[4] / $r1->[4]) * 100));

            printf("\n");
        }
    }
}

sub _color {
    my($num) = @_;
    if($num > 102) {
        return(color("red").sprintf("%6s%%", sprintf("%.2f", $num)).color("reset"));
    }
    if($num < 98) {
        return(color("green").sprintf("%6s%%", sprintf("%.2f", $num)).color("reset"));
    }
    return(sprintf("%6s%%", sprintf("%.2f", $num)));
}

sub _parse {
    my @data = @_;
    my $hashed = {};
    for my $line (@data) {
        if(my @res = $line =~ m/^([^\s]+)\s+(\d+)\s+(\d+)\s+ns\/op\s+(\d+)\s+B\/op\s+(\d+)\s+allocs\/op/gmx) {
            $hashed->{$res[0]} = \@res;
        }
    }
    return($hashed);
}
