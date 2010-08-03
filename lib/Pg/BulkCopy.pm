
=head1 Pg::BulkCopy.pm and pg_bulkcopy.pl Version 0.14

=cut


use warnings;
use strict;
use Moose;
use feature ":5.10" ;
use Time::Piece ;
use DBI ;
use File::Copy ;

package Pg::BulkCopy;
our $VERSION = '0.14' ;
use Moose ;
	has 'dbistring' => ( isa => 'Str', is => 'rw', required => 1, ) ;
	has 'filename' => ( isa => 'Str', is => 'rw', required => 1, ) ;	
	has 'table' => ( isa => 'Str', is => 'rw', required => 1, ) ; 
	has 'dbisuser' => ( isa => 'Str', is => 'rw', required => 0, ) ;
	has 'dbipass' => ( isa => 'Str', is => 'rw', required => 0, ) ;	
	has 'workingdir' => ( 
		isa => 'Str', 
		is => 'rw', 
		default => '/tmp/', 
		trigger => sub {
			my $self = shift ;
			unless ( $self->{'workingdir'} =~ m|/$| )
				{ $self->{'workingdir'} = $self->{'workingdir'} . '/' }
			}
		) ;
	
	has 'tmpdir' => ( isa => 'Str', is => 'rw', default => '/tmp/', ) ;
#	has 'direction' => ( isa => 'Str', is => 'rw', default => 'load', ) ; #Not implemented.
	has 'batchsize' => ( isa => 'Int', is => 'rw', default => 10000, ) ;
	has 'errorlog' => ( isa => 'Str', is => 'rw', default => 0, ) ;
	
	# this makes the default to make empty strings null. set the value to none to have no nulls.
	has 'null' => ( isa => 'Str', is => 'rw', default => "\'\'", ) ;
	has 'iscsv' => ( isa => 'Int', is => 'rw', default => 0, ) ; 
# This has to be stripped for batching and the headers fed to the column method, which also must be written.	
	has 'csvheader' => ( isa => 'Int', is => 'rw', default => 0, ) ; 	
	has 'maxerrors' => ( isa => 'Int', is => 'rw', default => 10, ) ; 
	has 'debug' => ( isa => 'Int', is => 'rw', default => 1, ) ;
	has 'errcode' => ( isa => 'Int', is => 'ro', default => 0, ) ;
	has 'errstr' => ( isa => 'Str', is => 'ro', default => '' ) ;

sub BUILD { 
 	my $self = shift ; 
# Whether error log is omitted, only a file name given or a full path given --
#  Set a default if no error log is provided.
#  Append to the working directory if only a file name is provided.
#  Leave it alone if a path is provided.
# Once the location is known, it is opened as an append to file handle and stored in self->FH. 	
 	if ( $self->{'errorlog'} == 0 ) { $self->{'errorlog'} = $self->{'workingdir'} . 'pg_BulkCopy.ERR' } 
 	elsif ( $self->{'errorlog'} !~ m|/| ) { $self->{'errlogor'} = $self->{'workingdir'} . $self->{'errorlog'} } ;
 	open ( $self->{'FH'}, '>>' , $self->{'errorlog'} ) or die " can\'t open $self->{'errorlog'}\n" ;
 	my $t = localtime ;
 	$self->dbg( 1, "Debug Logging to $self->{'errorlog'} $t" ) ;
	}	

sub dbg {
	my $self =shift ;
	my $level = shift  ;
	my $FH = $self->{'FH'} ;
	if ( $self->{'debug'} >= $level ) { 
		foreach my $w ( @_ ) { say STDERR $w ; say $FH $w }
		}
	}

sub CONN { 
	my $self =shift ;
	unless ( defined $self->{ 'CONN' } ) { 
		$self->{ 'CONN' } = DBI->connect( 
			$self->{ 'dbistring' } ,
			$self->{ 'dbiuser' } ,
			$self->{ 'dbipass' } ) or die "Error $DBI::err [$DBI::errstr]" ;
		}
	return $self->{ 'CONN' } ;	
	}

sub TRUNC {
	my $self =shift ;
	my $truncstr = 'TRUNCATE ' . $self->{'table'} . ' ;' ;
	my $conn = $self->CONN() ;
	$self->dbg( 5, $truncstr ) ;
	my $DBH = $conn->prepare( $truncstr ) ;
	$DBH->execute ;
	if ( $DBH->err ) { return $DBH->errstr } 
	else { return 0 }
	}

sub _OPTIONSTR {
	my $self =shift ;
	my $optstr = '' ;
	if ( $self->{ 'iscsv' } ) { 
		$optstr = $optstr . 'CSV ' ; 
		if ( $self->{'csvheader'} ) { $optstr = $optstr . 'HEADER ' ; }		
		} ;
	
	unless ( $self->{'null'} eq 'none' ) { $optstr = $optstr . "NULL $self->{ 'null' } " ; } ;
	if ( length( $optstr ) > 1 ) { $optstr = 'WITH ' . $optstr . ';' ; }
	else  { $optstr = ';' } ; # Even with no opts will need to tack on trailing semicolon.
	$self->dbg( 5, 'Optionstr: ', $optstr ) ;
	return $optstr ;	
	}

sub DUMP {
	my $self =shift ;
	our $returnstring = '' ;
	our $returnstatus = 1 ;	# Use 1 for successful and negative nos for failure.
	my $filename = $self->{ 'workingdir' } . $self->{ 'filename' } ;
# $returnstring = $filename ;	
# return ( 11, "***** $filename \n****" ) ;
	my $jobfile = $self->{ 'tmpdir' } . 'BULKCOPY.JOB' ;	
	my $conn = $self->CONN() ; #DBI->connect( $dbistr, $dbiuser, $dbipass ) or die "Error $DBI::err [$DBI::errstr]" ;
	my $opts = $self->_OPTIONSTR()  ;
	my $dumpstr = "COPY $self->{'table'} TO \'$jobfile\' $opts"  ; 
	$self->dbg( 5, "Dump SQL", $dumpstr  );
	my $DBH = $conn->prepare( $dumpstr ) ;
	$DBH->execute ;
	if ( $DBH->err ) { 
		$returnstatus =  $DBH->err ; 
		$returnstring = "Database Error: " . $DBH->errstr ; 
		} else {
		use File::Copy ;
		copy( $jobfile, $filename ) ;
		unlink $jobfile ;
		}
	$self->{ 'errcode' } = $returnstatus ;
	$self->{ 'errstr' } = $returnstring ;	
	return ( $returnstatus ) ; 	
	} #DUMP

	
sub LOAD { 
my $dbg = '' ;
	my $self =shift ;
	our $returnstring = '' ;
	our $returnstatus = 1 ;	# Use 1 for successful and negative nos for failure.
	my $ErrorCount = 0 ;
	my $fname = $self->{ 'workingdir' } . $self->{ 'filename' } ;
	my $jobfile = $self->{ 'tmpdir' } . 'BULKCOPY.JOB' ;
	my $rejectfile = $fname . '.REJECTS' ;	
	open my $REJECT, ">$rejectfile" or die "Can't open Reject File $rejectfile" ;
#	my $errorfile = $self->{ 'workingdir' } . $self->{ 'errorlog' } ;
#	open my $ERROR, ">$errorfile" or die "Can't open Error log $errorfile" ;
	my $opts = $self->_OPTIONSTR()  ;
	my $loadstr = "COPY $self->{'table'} FROM \'$jobfile\' $opts"  ; 
	
#	$self->dbg( 3, 'LOAD setup', "\nFilename -- ", $fname, "\nrejects -- ", $rejectfile, 
#		"\nerrorfile -- ",  $errorfile, "\nWorking Job file -- " ,
#		$jobfile, "\nLOAD QUERY -- ",  $loadstr ) ;
	
	my $DoLOAD = sub { 
		my $conn = $self->CONN() ;
		my $DBH = $conn->prepare( $loadstr ) ;
		$DBH->execute ;
		if ( $DBH->err ) { 
			my $DBHE = $DBH->errstr ;
			# These chars might end up next to line or the numer we seek.
			# translate turns them to spaces so they don't interfere.
			$DBHE =~ tr/\:\,/  /d ;
			$self->dbg( 2, "DBI Error Encountered. Attempting to identify and reject bad record.\n", $DBHE ) ;
			my @ears = split /\s/, $DBHE  ;
			while ( @ears ) {
				my $ear = shift @ears;
					if ( $ear =~ m/line/i ) {
						$ear = shift @ears  ;
						if ( $ear =~ /^-?\d+$/ ) { 
							$self->dbg( 4, "Identified Error Line",  $ear ) ;
							return $ear ; } 
						} # if ( $ear =~ m/line/i )
				} # while ( @ears ) 
				die "Cannot parse line number from \n||$DBHE||n" ;
			} # if ( $DBH->err )
		return 0 ;
		} ; #  $DoLOAD = sub
			
	my $ReWrite = sub {
		my $badline = shift ;
		if ( stat "$jobfile.OLD" ) { unlink "$jobfile.OLD" } ;
		use File::Copy ;
		move( $jobfile, "$jobfile.OLD" ) ;
		open OLD, "<$jobfile.OLD" ;
		open JOB, ">$jobfile" ;
		my $lncnt = 0 ;
		$self->dbg( 3, "ReWrite is trying to rewrite a job file." ) ;
		while (<OLD>) {
			$lncnt++ ;
			if ( $lncnt == $badline ) { print $REJECT $_ } 
			else { print JOB $_ }
			} ;
		close JOB ; 
		close OLD ;
		$self->dbg( 3, "New Job File: $jobfile\n", "Old: $jobfile.OLD\n" ) ;
if ( $self->{'debug'} > 1) { `cat $jobfile > j1.txt` ; my $old = "$jobfile.OLD" ; `cat $old > j2.txt` }
		unlink "$jobfile.OLD" ;
		} ;	
	open my $FH, "<$fname" or die "Unable to read $fname\n" ;
	my $batchsize = $self->{ 'batchsize' } ;
	my $jobcount = 0 ;
	my $batchcount = 0 ;
	my $finished = 0 ;
	my $iterator = 1 ;
	until ( $finished == 1 ) {
		# This is normally desired spew, leave debug at 1, call with debug 0 to suppress.
		$self->dbg( 1, "Processing Batch: $iterator" ) ;
		$batchcount = 0 ;
		open my $JOB, ">$jobfile" or die "Check Permissions on $self->{ 'tmpdir' } $!\n" ;
		while ( $batchcount < $batchsize ) {
			my $line = <$FH> ;
			print $JOB $line ;
			if ( eof($FH) ) { 
				$batchcount = $batchsize ; 
				$finished = 1 ; 
				say "Finished making batches" } ;				
			$batchcount++ ; $jobcount++ ;
			}
		close $JOB ;
		my $batchcomplete = 0 ;
		until ( $batchcomplete ) {
			my $loaded  = $DoLOAD->() ;
			if ( $loaded == 0 ) { $batchcomplete = 1 ; $iterator++ ; }
			else {  
				$ErrorCount++ ;
				if ( $ErrorCount >= $self->{ 'maxerrors' } ) {
					unlink $jobfile ;
					$finished = 1 ;
					$returnstring = 
#					"Max Errors $ErrorCount reached at $jobcount lines. See $errorfile and $rejectfile."  ;
					$returnstatus  = -1 ;
					$self->TRUNC() ;
#					print $ERROR $returnstring ;
					}
				else { $ReWrite->( $loaded ) ; }
				} ; 
	
		} # until batchcomplete
	} #until finished
	unlink $jobfile ;
	$self->{ 'errcode' } = $returnstatus ;
	$self->{ 'errstr' } = $returnstring ;	
#	return ( $returnstatus, $returnstring ) ; 
	return ( $returnstatus ) ;
}	 #LOAD

=head1 Warning this is a Pre-release version

The only methods reliably implemented are the simple forms of LOAD and DUMP, most options will fail. The current pg_BulkCopy.pl is placeholder. Since I started writing this, I seem to not have time to finish it, and decided it was functional enough that other people might find it useful even in its present state. 

=head1 pg_BulkCopy.pl

The utility script pg_BulkCopy.pl was written to provide postgreSQL with a convient bulk loading utility. The script is implemented as a wrapper and a module (pg_BulkCopy.pl) so that other programmers may easily incorporate the two useful methods LOAD and DUMP directly into other perl scripts. 

The advantage of this script over other scripts that have been written for this purpose is that if you can connect to and perform insert and delete operations on your database through the standard DBI interface (and it shouldn't matter which of the several postgres driver's you are using), pg_BulkCopy should just work.

The DUMP Method invokes postgres' COPY TO command, and does nothing useful in addition except copying the dump from the temp directory (because postgres may not have permission on where you want the file). You can choose Tab Delimited Text or CSV with whatever delimiter you want and a Null string of your choice.

The LOAD Method is much more interesting, it breaks the load file into chunks of 10000 (configurable) records in the temp directory and tries to COPY FROM, if it fails, it parses the error message for the line number, then it removes the failed line to a rejects file and tries again. As with DUMP you can select the options supported by the postgres COPY command, you can also set a limit on bad records (default is 10).

=head2 Module Pg::BulkCopy 

All methods used by pg_BulkCopy.pl are provided by Pg::BulkCopy

=head2 Systems Supported

This utility is specific to postgreSQL. It is a console application for the server itself. While in theory it could with some cleverness run remotely, such actions will be "unsupported". The utility is targeted towards recent versions of postgres running on unix-like operating systems, if you need to run it on Windows good luck and send a testing report if it works!

=head2 Methods for Pg::BulkCopy

=head2 CONN

Returns the dbi connection, initializing it if necessary. 

=head2 TRUNC

If the Trunc option is specified, delete all records from table with the postgres TRUNCATE command, instead of carrying out a LAOD or DUMP operation. 

=cut

=head2 LOAD

The main subroutine for importing bulk data into postgres.

=cut

=head2 DUMP

The main subroutine for exporting bulk data into postgres.

=cut


=head1 Troubleshooting and Issues:

=head2 Permissions Issues

The most persistent problem in getting Pg::BulkCopy to work correctly is permissions. First one must deal with hba.conf. Then once you are able to connect as the script user to psql and through a dbi connection you must deal with the additional issue that you are probably not running the script as the account postgres runs under. The account executing the script must be able to read and execute the script directories, read and write the working directory and the temp directory. Finally the account running the Postgres server must be able to read and write in the temp directory. It is for this reason that the script copies all work in process to the temp directory, which is defaulted to /tmp because this is a good place for it and because the default permissions might even be compatible. 

=head1 Testing

To properly test the module and script it is necessary to have an available configured database. So that the bundle can be installed silently through a cpan utility session no meaningful tests are run during installation. Proper testing of the module, and optionally the wrapper script must be done manually. 

=head2 Create and connect to the database

First make sure that the account you are using for testing has sufficient rights on the server. The sql directory contains a few useful scripts for creating a test database. On linux a command like this should be able to create the database: 
C<psql postgres > E<lt> C<create_test.sql>. C<dbitest.pl> adds a row to your new database and then deletes it, use dbitest to verify your dbi string and that can access the database.

=head2 The real tests are in t2
	
Edit the file t2/test.cfg. You will need to provide the necessary dsn values for the dbi connection.

Execute harness.sh from the distribution directory to run the tests.

=head2 Private subroutines

=head3 BUILD 

Is a moose component, it is run "after new".

=head3 dbg 

is used internally for outputting to stderr and the log file.

=cut

=head1 AUTHOR

John Karr, C<< <brainbuz at brainbuz.org> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-pg-bulkcopy at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=Pg-BulkCopy>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.




=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc Pg::BulkCopy


You can also look for information at:

=over 4

=item * RT: CPAN's request tracker

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=Pg-BulkCopy>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/Pg-BulkCopy>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/Pg-BulkCopy>

=item * Search CPAN

L<http://search.cpan.org/dist/Pg-BulkCopy/>

=back


=head1 ACKNOWLEDGEMENTS


=head1 LICENSE AND COPYRIGHT

Copyright 2010 John Karr.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 3 or at your option
any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

A copy of the GNU General Public License is available in the source tree;
if not, write to the Free Software Foundation, Inc.,
59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.


=cut

1; # End of Pg::BulkCopy