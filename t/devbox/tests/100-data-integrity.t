use strict;
use warnings;
use Test::More;
use lib('t');
use TestUtils;
use lib('/box/tests/');
use LMDTests;

plan tests => 12;

###########################################################
{
    my $query = 'GET services
Columns: contacts
Filter: host_name = test
Filter: description = Ping';
    my $exp  = [[[
          "con1",
          "con2",
          "con3",
          "con4",
          "con5",
          "example"
    ]]];
    LMDTests::lq_tests("services contacts", $query, $exp);
};

###########################################################

{
    my $query = 'GET services
Columns: contacts
Filter: host_name = test
Filter: description = Load';
    my $exp  = [[[
          "con1",
          "con10",
          "con2",
          "con3",
          "con4",
          "con5",
          "con6",
          "con7",
          "con8",
          "con9",
          "example"
    ]]];
    LMDTests::lq_tests("services contacts", $query, $exp);
};

###########################################################
