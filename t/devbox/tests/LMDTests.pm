package LMDTests;

use strict;
use warnings;
use Cpanel::JSON::XS;
use File::Temp qw(tempfile);
use Test::More;

###########################################################
sub get_json {
    my $json = Cpanel::JSON::XS->new->utf8(1);
    return $json;
}

###########################################################

sub lq_tests {
    my($name, $query, $exp) = @_;

    ok(1, $name);
    my $test = {
        cmd   => '/src/lq tmp/thruk/lmd/live.sock',
        stdin => $query,
        like  => ['/\[/'],
    };
    TestUtils::test_command($test);
    my $json = &get_json();
    my $res  = $json->decode($test->{stdout});
    is_deeply($res, $exp, $name.' query result') || do {
        my $expStr = $json->pretty->encode($exp);
        my $resStr = $json->pretty->encode($res);
        my $diff = &diff($expStr, $resStr);
        diag("Exp:\n".$expStr."\n"
            ."Got:\n".$resStr."\n"
            ."Diff:\n".$diff
        );
    };
}

###########################################################
sub diff {
    my($str1, $str2) = @_;
    my($fh1, $f1) = tempfile();
    my($fh2, $f2) = tempfile();

    print $fh1 $str1;
    print $fh2 $str2;

    close $fh1;
    close $fh2;
    my $diff = `diff -u $f1 $f2`;

    unlink($f1, $f2);

    $diff =~ s/^\-\-\-\ .*\n//mx;
    $diff =~ s/^\+\+\+\ .*\n//mx;

    return $diff;
}
###########################################################

1;
