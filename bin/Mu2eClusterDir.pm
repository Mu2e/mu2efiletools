#!/usr/bin/perl -w
#
# A common piece of code to iterate over all job-level subdirectories
# in a mu2eprodsys cluster dir output structure, and find them all,
# even if if subdirs are moved or deleted by the calling script.
#
# A.Gaponenko, 2016
#

package Mu2eClusterDir;
use strict;
use Exporter qw( import );
use Carp;

#================================================================
# mu2eprodsys cluster structure limits the number of subdirs to 1000,
# so it is OK to read them all in memory.  This function
# is used to wrap the error handling.
sub getDirEntries($) {
    my ($dirname) = @_;

    # readdir(3) may return NULL on both success and failure.  The call
    # does not change errno on success, but sets it on failure.
    #
    # I do not know if there is any error diagnostic for the
    # incomplete directory listing issue on /pnfs. Try to catch it
    # by pre-setting errno to 0 and testing it after readdir().

    opendir(my $CLD0, $dirname) or croak "Can't opendir($dirname): $! on ".localtime()."\n";
    $! = 0;
    my @list0 = readdir($CLD0);
    croak "Error in readdir($dirname): $! on ".localtime()."\n" if $!;
    closedir($CLD0) or croak "Can't closedir($dirname): $! on ".localtime()."\n";

    return sort grep { ($_ ne '.') and ($_ ne '..') } @list0;
}

#================================================================
sub iterate {
    my ($clusterdir, $f, @args) = @_;

    $clusterdir =~ s|/$||;

    # 00, 01, ...
    my @dirlist0 = getDirEntries($clusterdir);
    foreach my $sd1 (@dirlist0) {
        next if $sd1 =~ /^\./;

        # 00000, ...
        my @dirlist1 = getDirEntries("$clusterdir/$sd1");
        foreach my $sd2 (@dirlist1) {
            next if $sd2 =~ /^\./;

            my $jobdir= "$clusterdir/$sd1/$sd2";
            $f->($jobdir, @args);
        }
    }
}

#================================================================
1;
