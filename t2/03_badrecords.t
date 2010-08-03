#!perl 
use feature ':5.10';
use Cwd ;
use Config::Std;
use IhasQuery ;
use File::Copy ;
use strict ;

use Test::More tests => 23;
#use Test::More 'no_plan' ;


########################################################################
# COMMON CODE. CUT AND PASTE TO ALL TESTS.
#######


diag( "Testing Pg::BulkCopy $Pg::BulkCopy::VERSION, Perl $], $^X" );
BEGIN {
    use_ok( 'Pg::BulkCopy' ) || print "Bail out!";
    }

# get pwd, should be distribution directory where harness.sh was invoked from.
my $pwd = getcwd;
my $tdata = "$pwd/tdata" ;

# Load named config file into specified hash...
read_config "$pwd/t2/test.conf" => my %config;

# Extract the value of a key/value pair from a specified section...
my $dbistr  = $config{DBI}{dbistr};
my $dbiuser  = $config{DBI}{dbiuser};
my $dbipass = $config{DBI}{dbipass};

########################################################################
# END OF THE COMMON CODE
#######

# Run a basic test with the default tab seperated file.
#  Checks that the right number of records imported, 
# verifies the reject file.

my $table = 'millions' ;
my $filename = "errors_25.tsv" ;

my $bad5 = Pg::BulkCopy->new(
    dbistring => $dbistr,
    dbiuser   => $dbiuser,
    dbipass   => $dbipass,
    filename  => $filename,
    workingdir => "$tdata/",
    debug      => 1 ,
    table     => $table, );

my $millions = IhasQuery->new( $bad5->CONN(), 'millions' ) ;
ok( $millions, "Have a working IHQ object from PGBCP object." ) ;

$bad5->TRUNC() ;
$bad5->LOAD() ;
is ( $bad5->errcode() , 1 , 'Return Status should be 1' ) ;
is ( $bad5->errstr() , '' , 'Return string should be empty' ) ;
is ( $millions->count() , 22, "Counted 22 records loaded." ) ;

open FH, "<$tdata/$filename.REJECTS" or die $!;
like (  <FH> , 
        qr/1	1368689183	97	reconciler/,
        "reject matches: 1	1368689183	97	reconciler") ;
like (  <FH> , 
        qr/12	5728931094	92	fluctuating	ZAPPA/,
        "reject matches: 12	5728931094	92	fluctuating	ZAPPA") ;
like (  <FH> , 
        qr/25	Jubilee/,
        "reject matches: 25	Jubilee") ; 
my $temp = <FH> ;
my $len = length $temp ;
say "Extra line $temp, $len" ;
ok ( $len <= 1 , "Testing that if there is an extra line, it should be empty" ) ;
ok( eof FH == 1 , "Confirm end of reject file" );                        

close FH ;



# Similar to above except that a different csv file is used.

$filename = "errors_25.csv" ;
$bad5->iscsv(1);
$bad5->filename($filename) ;
$bad5->LOAD() ;
is ( $bad5->errcode(), 1 , 'Return Status should be 1' ) ;
is ( $bad5->errstr() , '' , 'Return string should be empty' ) ;
is ( $millions->count() , 42, "Counted 42 records loaded." ) ;


open FH, "<$tdata/$filename.REJECTS" or die $!;
like (  <FH> , 
        qr/33,2791161243,182,Locke/,
        "reject matches: 33,2791161243,182,Locke") ;
like (  <FH> , 
        qr/36,3650751672,217,regenerates, degenerate/,
        "reject matches: 36,3650751672,217,regenerates, degenerate") ;                    
like (  <FH> , 
        qr/7,1784270707,182,hammering/,
        "reject matches: 7,1784270707,182,hammering") ;     
like (  <FH> , 
        qr/124,tyrant,/,
        "reject matches: 124,tyrant,") ; 
like (  <FH> , 
        qr/125,040403232,318,"Macaroni Macaroon",,/,
        "reject matches: 125,040403232,318,\"Macaroni Macaroon\",,") ; 
$temp = <FH> ;
$len = 0 ;
$len = length $temp ;
ok ( $len <= 1 , "Testing that if there is an extra line, it should be empty" ) ;
ok( eof FH == 1 , "Confirm end of reject file" );                      

close FH ;

# This is just an extra test run of a different file.

$filename = "t157.csv" ;
$table = 'testing' ;

my $PG_Test1 = Pg::BulkCopy->new(
    dbistring => $dbistr,
    dbiuser   => $dbiuser,
    dbipass   => $dbipass,
    filename  => $filename,
    workingdir => "$tdata/",
    iscsv       => 1,
    table     => $table, );

is( $PG_Test1->TRUNC(), 0, ) ;
is ( $PG_Test1->LOAD() , 1 , 'Return Status should be 1' ) ;
my $testing1 = IhasQuery->new( $PG_Test1->CONN() , 'testing' ) ;
is ( $testing1->count() , 156 , "Should load 156" ) ;

unlink "$tdata/blob1.tsv.REJECTS" ;
unlink "$tdata/pg_BulkCopy.ERR" ;
unlink "$tdata/blob2.csv.REJECTS" ;
unlink "$tdata/DUMP1.tsv" ;
unlink "$tdata/DUMP1.csv" ;
unlink "$tdata/DUMP1.csv.REJECTS" ;
unlink "$tdata/DUMP1.tsv.REJECTS" ;
unlink "$tdata/t133992.tsv.REJECTS" ;


