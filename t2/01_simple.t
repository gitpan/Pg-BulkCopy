#!perl 
use feature ':5.10';
use Cwd ;
use Config::Std;
use IhasQuery ;
use File::Copy ;

use Test::More tests => 29;
#use Test::More 'no_plan' ;

# require 't2/PGBCP-Common-Test.pm' ;
# import Common ;

diag( "Testing Pg::BulkCopy $Pg::BulkCopy::VERSION, Perl $], $^X" );
BEGIN {
    use_ok( 'Pg::BulkCopy' ) || print "Bail out!";
    }

########################################################################
# COMMON CODE. CUT AND PASTE TO ALL TESTS.
#######

# get pwd, should be distribution directory where harness.sh was invoked from.
my $pwd = getcwd;
my $tdata = "$pwd/tdata" ;

# Load named config file into specified hash...
read_config "$pwd/t2/test.conf" => my %config;

# Extract the value of a key/value pair from a specified section...
my $dbistr  = $config{DBI}{dbistr};
my $dbiuser  = $config{DBI}{dbiuser};
my $dbipass = $config{DBI}{dbipass};
my $table = 'testing' ;

########################################################################
# END OF THE COMMON CODE
#######

# Run a basic test with the default tab seperated file.

my $filename = "blob1.tsv" ;

my $PG_Test1 = Pg::BulkCopy->new(
    dbistring => $dbistr,
    dbiuser   => $dbiuser,
    dbipass   => $dbipass,
    filename  => $filename,
    workingdir => "$tdata/",
    icsv      => 0 ,
    table     => $table, );

ok( $PG_Test1, "Have a variable for new object" ) ;
say "\n****\n****\n $tdata/ \n" , $PG_Test1->{'workingdir'} ;
# Tests the trigger on workingdir. the first test object uses the mandatory trailing slash!
is ( $PG_Test1->{'workingdir'} , "$tdata/" , "Confirm working directory is set to $tdata/" ) ;


is( $PG_Test1->TRUNC(), 0, ) ;

#my @Z1 = $PG_Test1->LOAD() ;
is ( $PG_Test1->LOAD() , 1 , 'Return Status should be 1' ) ;
is ( $PG_Test1->{'errcode'} , 1 , 'Internal Error Code should also be 1' ) ;
is ( $PG_Test1->{'errstr'} , '' , 'Return string should be empty' ) ;

my $testing = IhasQuery->new( $PG_Test1->CONN() , 'testing' ) ;
is ( $testing->count() , 15631 , "Should load 15631" ) ;

$PG_Test1->filename( 'DUMP1.tsv' ) ;
is( $PG_Test1->filename(), 'DUMP1.tsv', 'Successfully set a new filename for new operation') ;
$PG_Test1->DUMP() ;
is ( $PG_Test1->errcode() , 1 , 'Return Status should be 1' ) ;
is ( $PG_Test1->errstr() , '' , 'Return string should be empty' ) ;


my @S1 = stat "$pwd/tdata/$filename" ;
my @S2 = stat "$pwd/tdata/DUMP1.tsv" ;
say "sizes $S1[7] $S2[7]" ;
ok(  $S1[7] == $S2[7] , "The dump and load file should by the same size, but records may be in different order." ) ;

# Now do a test with a csv file.

$filename = "blob2.csv" ;

my $PG_Test2 = Pg::BulkCopy->new(
    dbistring => $dbistr,
    dbiuser   => $dbiuser,
    dbipass   => $dbipass,
    filename  => $filename,
    workingdir => "$tdata",
    iscsv     => 1, 
    table     => $table, );

ok( $PG_Test2, "Have a variable for new object" ) ;

# Tests the trigger on workingdir. The second object omits the mandatory trailing slash!
# The trigger should fix it so that users may include or omit the trailing slash.
is ( $PG_Test2->{'workingdir'} , "$tdata/" , "Confirm working directory is set to $tdata/" ) ;

# my $Z = 0 ; 
# my @Z1 = () ; 
# my @Z2 = () ; 
@S1 =() ; 
@S2 = () ;

# clear the table.
is( $PG_Test2->TRUNC(), 0, "TRUNC thinks it ran" ) ;
is( $testing->count(), 0, 'Confirm that Trunc worked.') ;

is ( $PG_Test2->LOAD() , 1 , 'Return Status should be 1' ) ;
is ( $PG_Test2->errstr() , '' , 'Return string should be empty' ) ;
is ( $testing->count() , 33992 , "Should load 33992, old records should have been truncated." ) ;

$PG_Test2->filename( 'DUMP1.csv' ) ;
is( $PG_Test2->filename(), 'DUMP1.csv', 'Successfully set a new filename for new operation') ;
is ( $PG_Test2->DUMP() , 1 , 'Return Status should be 1' ) ;
is ( $PG_Test2->errstr() , '' , 'Return string should be empty' ) ;
my @S1 = stat "$pwd/tdata/$filename" ;
my @S2 = stat "$pwd/tdata/DUMP1.csv" ;
say "sizes $S1[7] $S2[7]" ;
ok(  $S1[7] == $S2[7] , "The dump and load file should by the same size, but records may be in different order." ) ;

$PG_Test2->filename( 't133992.tsv' ) ;
$PG_Test2->iscsv( 0 ) ;
$PG_Test2->table( 'millions' ) ;
is( $PG_Test2->table(), 'millions', 'confirm attribute set' ) ;
$PG_Test2->TRUNC() ;
my $millions = IhasQuery->new( $PG_Test2->CONN() , 'millions' ) ;
is( $millions->count(), 0, 'Confirm truncation' ) ;
$PG_Test2->LOAD() ;
is( $millions->count(), 133992, 'Confirm Load of 133992' ) ;

# This object doesn't run a real test, it just checks some triggers, defaults and accessors

my $PG_Test3 = Pg::BulkCopy->new(
    dbistring => $dbistr,
    dbiuser   => $dbiuser,
    dbipass   => $dbipass,
    filename  => $filename,
    table     => $table, );    
is ( $PG_Test3->workingdir() , '/tmp/', 'workingdir defualts to tmp' ) ;
is ( $PG_Test3->iscsv() , 0, 'iscsv defaults to 0, for tab seperated.' ) ;
is ( $PG_Test3->errorlog() , '/tmp/pg_BulkCopy.ERR', 'Error log file in working directory.' ) ;    

unlink "$tdata/blob1.tsv.REJECTS" ;
#unlink "$tdata/pg_BulkCopy.ERR" ;
unlink '/tmp/pg_BulkCopy.ERR' ;
unlink "$tdata/blob2.csv.REJECTS" ;
unlink "$tdata/DUMP1.tsv" ;
unlink "$tdata/DUMP1.csv" ;
unlink "$tdata/DUMP1.csv.REJECTS" ;
unlink "$tdata/DUMP1.tsv.REJECTS" ;
unlink "$tdata/t133992.tsv.REJECTS" ;
  
