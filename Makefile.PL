use inc::Module::Install;

name     'Pg-BulkCopy';
all_from 'lib/Pg/BulkCopy.pm';
author   q{John Karr <brainbuz@brainbuz.org>};
license  'gpl';

build_requires 'DBD::Pg';
build_requires 'Moose';
perl_version '5.12';
build_requires 'Test::More';
build_requires 'Test::Exception' ;
build_requires 'File::Copy' ;
build_requires 'FileHandle';
build_requires 'IO::CaptureOutput' ;
build_requires 'IhasQuery';
build_requires 'Config::Merge';
build_requires 'Config::General' ;
build_requires 'Cwd' ;
build_requires 'Getopt::Long' ;

auto_install;

WriteAll;

