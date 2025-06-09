use strict;
use warnings;
use Test::More;
use lib('t');
use TestUtils;
use lib('/box/tests/');
use LMDTests;

plan tests => 18;

###########################################################
{
    my $query = 'GET services
Columns: host_name description custom_variables
Filter: host_name = test
Filter: custom_variable_names !=';
    my $exp  = [
        [ 'test', 'Load', {
            'TEST1' => 'test2',
            'TEST2' => 'def',
            'TEST3' => 'xxx',
            'TEST4' => 'yyy'
        }],
        [ 'test', 'Ping', {
            'TEST1' => 'test2',
            'TEST2' => 'def',
            'TEST3' => 'xxx',
            'TEST4' => 'yyy'
        }]
    ];
    LMDTests::lq_tests("services custom_variables", $query, $exp);
};

###########################################################
{
    my $query = 'GET services
Columns: host_name description custom_variable_names
Filter: host_name = test
Filter: custom_variable_names !=';
    my $exp  = [
        [ 'test', 'Load', [ 'TEST1', 'TEST2', 'TEST3', 'TEST4' ] ],
        [ 'test', 'Ping', [ 'TEST4', 'TEST3', 'TEST2', 'TEST1' ] ]
    ];
    LMDTests::lq_tests("services custom_variable_names", $query, $exp);
};

###########################################################
{
    my $query = 'GET services
    Columns: host_name description custom_variable_values
    Filter: host_name = test
    Filter: custom_variable_names !=';
    my $exp  = [
          [ 'test', 'Load', [ 'test2', 'def', 'xxx', 'yyy' ] ],
          [ 'test', 'Ping', [ 'yyy', 'xxx', 'def', 'test2' ] ]
        ];
    LMDTests::lq_tests("services custom_variable_values", $query, $exp);
};

###########################################################
