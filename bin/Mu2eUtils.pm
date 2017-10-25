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
use Cwd 'abs_path';
use Digest;
use Compress::Zlib qw(adler32);
use Fcntl;

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

#================================================================
sub filterLocationCookie($) {
    my ($fi) = @_;
    my $cookie = $fi->location_cookie;
    $cookie =~ s/_//g;
    $cookie =~ m/^\d+$/ or die "Unexpected format of location_cookie='",
    $fi->cookie,"' for a non-SFA file.";
    return Mu2eEnstoreInfo->new(label=>$fi->label, location_cookie=> 0 + $cookie);
}

#================================================================
# getEnstoreInfo($filename) returns a Mu2eEnstoreInfo
# if the file has a tape label, or undef.

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
sub dCacheChecksum($) {
    my $fn = shift;
    my $ckn = dirname($fn) . '/.(get)('.basename($fn).')(checksum)';

    my $currentTry = 0;
    my $delay = 5; # seconds

    my $res = '';

  CKLOOP: while(1) {
      if(open(my $fh, '<', $ckn)) {
          $res = <$fh>;
          chomp $res;
          return $res if $res ne '';
      }
      else {
          warn "Warning: could not open pnfs checksum file $ckn : $! on ",scalar(localtime()),"\n";
      }

      ++$currentTry;
      if($currentTry >= 3) {
          die "ERROR: could not get dCacheChecksum() after $currentTry tries.\n";
      }
      else {
          print "dCacheChecksum($fn): will retry in $delay seconds\n";
      }

      sleep $delay;
      $delay *= 2;
  }
}

#================================================================
# Copies $infile to an $outfile on /pnfs, computing an adler32
# checksum on bytes in transit.  The checksum is compared against
# dCache-computed value for $outfile, and, if available, for $infile.
#
# A pre-requisite: the destination directory must exist.
#
sub checked_copy {
    my ($infile, $outfile, $inopts) = @_;

    my $opts = $inopts // {};
    my $sha256 = $$opts{'sha256'};

    my $dig = $sha256 ? Digest->new('SHA-256') : undef;

    sysopen(my $in, $infile, O_RDONLY)
        or die "Can not open input \"$infile\": $! on ".localtime()."\n";

    sysopen(my $out, $outfile, O_WRONLY | O_EXCL | O_CREAT)
        or die "Can not create output \"$outfile\": $! on ".localtime()."\n";

    my $blocksize = 4*1024*1024;
    my ($rst, $crc);
    while($rst = sysread($in, my $buf, $blocksize)) {
        $crc = adler32($buf, $crc);
        $dig->add($buf) if($dig);
        syswrite($out, $buf)
            or die "Error writing to \"$outfile\": $! on ".localtime()."\n";
    }

    die "Error reading \"$infile\": $! on ".localtime()."\n"
        unless defined $rst;

    close $out or die "Error closing output \"$outfile\": $!  on ".localtime()."\n";
    close $in  or die "Error closing input \"$infile\": $!  on ".localtime()."\n";

    my $readcheck = sprintf "ADLER32:%08x", $crc;

    my $dstcheck = dCacheChecksum($outfile);
    die "Detected data corruption on write: dst checksum = $dstcheck != read checksum $readcheck,"
        . " writing \"$outfile\" on ".localtime()."\n"
        if($dstcheck ne $readcheck);

    if(abs_path($infile) =~ m|^/pnfs|) {
        my $srccheck = dCacheChecksum($infile);
        die "Detected data corruption on read: read checksum = $readcheck != source checksum $srccheck\n"
            if($srccheck ne $readcheck);
    }

    if($sha256) {
        $$sha256 = $dig->hexdigest;
    }

    return $readcheck;
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
