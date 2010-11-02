#!perl 

use Cwd ;
use Test::More tests => 5 ;
#use Test::More 'no_plan' ;
use Test::Exception ;
use IO::CaptureOutput qw/capture_exec/ ;

BEGIN {
  use_ok( 'Pg::BulkCopy' ) || print "Bail out!";
}



# This test validates at install time that the script would be runable.
# To validate that it will *work* in your environment please review the
# documentation on using the tests in t2. 11_script.t contains the testing
# specific to pg_bulkcopy.pl.


# Your script may be somewhere else! 
my $pwd = getcwd;
my $script = "$pwd/bin/pg_bulkcopy.pl" ;

ok( require( $script ) , 'loaded file ok' ) or exit ;

throws_ok { main() } qr/information about/, 'should give a help message without arguments'  ;

my $cmd = qq /$script --filename tdata\/shaved.csv --iscsv 1 --dbistring "DBI:Pg:dbname=pg_bulkcopy_test;host=127.0.0.1" --table testing --debug 3 / ;

if ( stat "01_load.out1" ) 
		{ unlink "01_load.out1"  or
			die "Can\t delete existing file 01_load.out1 $! " } ;
# The failure of this test (since without a configured postgres server it can't succeed)
# has pointed out the need to in a future release be more graceful in death.
if ( stat "01_load.out2" ) 
		{ unlink "01_load.out2"  or
			die "Can\t delete existing file 01_load.out2 $! " }			
			
system( "$cmd 1> 01_load.out1" ) ;
system( "$cmd 2> 01_load.out2" ) ;
open RES, "<01_load.out1" ;
 my $result  = '' ;
 while (<RES>) { $result = $result . $_ } ; 
 close RES ;
ok( $result =~ m/dbistring DBI:Pg:dbname=pg_bulkcopy_test;host=127.0.0.1/ , 
	'Output shows the script read the dbistring value' ) ;
ok( $result =~ m/temporary Directory is not specified/ , 
	'Told us we didnt specify temp directory' );

1;