use inc::Module::Install;

name     'Pg-BulkCopy';
all_from 'lib/Pg/BulkCopy.pm';
author   q{John Karr <brainbuz@brainbuz.org>};
license  'gpl';

build_requires 'Test::More';
build_requires 'DBD::Pg';
build_requires 'Moose';
#build_requires 'Config::Merge';
perl_version '5.10';
auto_install;

WriteAll;

