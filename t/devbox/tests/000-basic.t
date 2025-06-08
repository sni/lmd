use strict;
use warnings;
use Test::More;

BEGIN {
    use lib('t');
    require TestUtils;
    import TestUtils;
}

plan tests => 8;

###########################################################
# verify that we use the correct binary
TestUtils::test_command({
    cmd  => '/bin/bash -c "type lmd"',
    like => ['/\/src\/lmd/'],
}) or BAIL_OUT("wrong lmd path");

TestUtils::test_command({
    cmd  => '/bin/bash -c "omd status lmd"',
    like => ['/lmd:.*running/'],
});

###########################################################
