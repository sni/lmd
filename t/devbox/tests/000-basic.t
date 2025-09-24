use strict;
use warnings;
use Test::More;

BEGIN {
    use lib('t');
    require TestUtils;
    import TestUtils;
}

use lib('/box/tests/');
use LMDTests;

plan tests => 24;

###########################################################
# verify that we use the correct binary
TestUtils::test_command({
    cmd  => '/bin/bash -c "type lmd"',
    like => ['/\/src\/lmd/'],
}) or BAIL_OUT("wrong lmd path");

TestUtils::test_command({
    cmd  => '/bin/bash -c "omd restart lmd"',
    like => ['/Starting\ lmd\.\.\.OK/'],
});

TestUtils::test_command({
    cmd  => '/bin/bash -c "omd status lmd"',
    like => ['/lmd:.*running/'],
});

###########################################################
sleep(1); # wait for lmd to start

###########################################################
{
    my $query = 'GET sites
Columns: peer_name status
Sort: peer_name asc';
    my $exp  = [['icinga2', 0], ['naemon', 0]];
    LMDTests::lq_tests("sites query", $query, $exp);
};

###########################################################
{
    my $query = 'GET services
Columns: host_name description';
    my $exp  = [
          [ 'icinga2', 'disk' ],
          [ 'icinga2', 'disk /' ],
          [ 'icinga2', 'http' ],
          [ 'icinga2', 'ping4' ],
          [ 'icinga2', 'ping6' ],
          [ 'icinga2', 'ssh' ],
          [ 'naemon', 'Disk /' ],
          [ 'naemon', 'Load' ],
          [ 'naemon', 'Ping' ],
          [ 'naemon', 'Users' ],
          [ 'test', 'Disk /' ],
          [ 'test', 'Load' ],
          [ 'test', 'Ping' ],
          [ 'test', 'Users' ]
        ];
    LMDTests::lq_tests("services query", $query, $exp);
};

###########################################################
