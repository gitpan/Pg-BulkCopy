use inc::Module::Install;

name     'Pg-BulkCopy';
all_from 'lib/Pg/BulkCopy.pm';
author   q{John Karr <brainbuz@brainbuz.org>};
license  'gpl';

build_requires 'DBD::Pg';
build_requires 'Moose';
perl_version '5.10';
build_requires 'Test::More';
build_requires 'FileHandle';
build_requires 'IhasQuery';
build_requires 'Config::Merge';
build_requires 'Config::Std' ;
auto_install;

WriteAll;

