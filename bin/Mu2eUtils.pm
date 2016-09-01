#!/usr/bin/perl -w
#
# Some common functions for the package.
#
# A.Gaponenko, 2016
#

package Mu2eUtils;
use strict;
use Exporter qw( import );
use Carp;
use File::Basename;
use Digest;

use Class::Struct Mu2eEnstoreInfo => [label=>'$', location_cookie=>'$'];

#================================================================
sub tapeBacked($) {
    my ($path) = @_;
    return $path =~ /tape/;
}

#================================================================
sub makeSamLocation($$) {
    my ($pathname, $tapeinfo) = @_;
    my $dir = dirname($pathname);

    if(tapeBacked($pathname)) {
        return 'enstore:'.$dir.'('.$tapeinfo->location_cookie.'@'.$tapeinfo->label.')';
    }
    else {
        return "dcache:".$dir   if $dir =~ m|^/pnfs/mu2e/persistent|;
        return "dcache:".$dir   if $dir =~ m|^/pnfs/mu2e/scratch|;
        die "Error: makeSamLocation('$pathname'): unknown file location.\n";
    }
}

#================================================================
sub queryBFID($) {
    my ($bfid) = @_;

    my $file_info = `enstore info --bfid $bfid`;
    die "Error from enstore info for BFID $bfid\n" unless defined $file_info;

    $file_info =~ m/'external_label'\s*:\s*'(.+)'/
        or die "Error parsing file info: can't extract external_label from:\n$file_info\n";
    my $label = $1;

    $file_info =~ m/'location_cookie'\s*:\s*'(.+)'/
        or die "Error parsing file info: can't extract location_cookie from:\n$file_info\n";
    my $cookie = $1;

    return ( Mu2eEnstoreInfo->new(label=>$label, location_cookie=> $cookie), $file_info );
}

sub filterLocationCookie($) {
    my ($fi) = @_;
    my $cookie = $fi->location_cookie;
    $cookie =~ s/_//g;
    $cookie =~ m/^\d+$/ or die "Unexpected format of location_cookie='",
    $fi->cookie,"' for a non-SFA file.";
    return Mu2eEnstoreInfo->new(label=>$fi->label, location_cookie=> 0 + $cookie);
}

my $enstore_checked = 0;
sub getEnstoreInfo($) {
    my ($pathname) = @_;

    if(not $enstore_checked) {
        my $out = `enstore -h`;
        die "Error running 'enstore' command\n" unless defined $out;
        $enstore_checked = 1;
    }

    my $file_bfid = `enstore pnfs --bfid $pathname 2>/dev/null`;
    if($? ne 0) {
        return undef; # the file is not on tape yet.
    }
    chomp $file_bfid;

    my ($fi, $file_info) = queryBFID($file_bfid);

    # Check for SFA.  Per cs-doc-4698v4 "Enstore Small File Aggregation User Guide"
    # external_label for a package member will contain ':'
    if($fi->label =~ /:/) {

        if($file_info =~ m/'package_id'\s*:\s*None/i) {
            return undef; # not packaged yet
        }

        $file_info =~ m/'package_id'\s*:\s*'(.+)'/
            or die "Error parsing file info: can't extract package_id from:\n$file_info\n";

        my $package_bfid = $1;

        my ($pkg, $pkg_info) = queryBFID($package_bfid);

        return filterLocationCookie($pkg);
    }

    return filterLocationCookie($fi);
}

#================================================================
sub hexdigest($) {
    my ($pathname) = @_;

    # compute sha256 for the file
    my $dig = Digest->new('SHA-256');
    open(my $fh, '<', $pathname) or die "Error opening $pathname for reading: $! on ".localtime()."\n";
    $dig->addfile($fh);
    close $fh;

    return $dig->hexdigest;
}

#================================================================
1;
