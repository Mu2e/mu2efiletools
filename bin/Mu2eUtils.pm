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

    my $l1fn = dirname($pathname) . '/.(use)(1)(' . basename($pathname) . ')';
    open(my $FH, '<', $l1fn) or return undef; # not on tape yet
    my $file_bfid = <$FH>;
    die "Can not get BFID from pnfs layer 1  $l1fn: $! on ".localtime()."\n"
        unless defined $file_bfid;
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
# Returns a dCache-computed checksum.  The file must be in /pnfs.
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
# Returns the dCache-style checksum by querying dCache or reading
# the file and computing the sum for files outside of dCache.
sub adler32Checksum($) {
    my $infile = shift;

    my $res;
    if(abs_path($infile) =~ m|^/pnfs|) {
        $res = dCacheChecksum($infile);
    }
    else {
        sysopen(my $in, $infile, O_RDONLY)
            or die "Can not open input \"$infile\": $! on ".localtime()."\n";

        my $blocksize = 4*1024*1024;
        my ($rst,$crc);
        while($rst = sysread($in, my $buf, $blocksize)) {
            $crc = adler32($buf, $crc);
        }
        die "Error reading \"$infile\": $! on ".localtime()."\n"
            unless defined $rst;

        close $in  or die "Error closing input \"$infile\": $!  on ".localtime()."\n";

        $res = sprintf "ADLER32:%08x", $crc;
    }

    return $res;
}

#================================================================
# Copies $infile to an $outfile on /pnfs, computing an adler32
# checksum on bytes in transit.  The checksum is compared against
# dCache-computed value for $outfile, and, if available, for $infile.
# If the 'sha256' option is requested, a sha256 checksum will also
# be computed and stored in the variable pointed to by the option.
#
# A pre-requisite: the destination directory must exist.
#
sub checked_copy {
    my ($infile, $outfile, $inopts) = @_;

    my $opts = $inopts // {};
    my $sha256 = $$opts{'sha256'};

    # !!! DANGER !!!  with maxtries > 1 the /pnfs file is deleted before a retry.
    # If an upload process gets stuck on /pnfs I/O and another script puts the same
    # file in, then the first process gets "unstuck" with an I/O error, the file will
    # be lost!
    my $maxtries = $$opts{'maxtries'} // 1;

    my $numtries = 0;

  COPYTRY:
    while(1) {
        ++$numtries;

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
            if(not syswrite($out, $buf)) {
                my $msg = "Error writing to \"$outfile\": $! on ".localtime()." (try $numtries)\n";
                if($numtries < $maxtries) {
                    warn $msg;
                    # this check is racy, but still better than doing nothing
                    die "The input file \"$infile\" is gone - stop now.\n" unless -r $infile;
                    print localtime().": Removing file $outfile\n";
                    unlink $outfile or die "Error unlinking \"$outfile\" on ".localtime().": $!\n";
                    next COPYTRY;
                }
                else {
                    die $msg;
                }
            }
        }

        die "Error reading \"$infile\": $! on ".localtime()."\n"
            unless defined $rst;

        if(not close $out) {
            my $msg = "Error closing output \"$outfile\": $!  on ".localtime()." (try $numtries)\n";
            if($numtries < $maxtries) {
                warn $msg;
                # this check is racy, but still better than doing nothing
                die "The input file \"$infile\" is gone - stop now.\n" unless -r $infile;
                print localtime().": Removing file $outfile\n";
                unlink $outfile or die "Error unlinking \"$outfile\" on ".localtime().": $!\n";
                next COPYTRY;
            }
            else {
                die $msg;
            }
        }

        close $in  or die "Error closing input \"$infile\": $!  on ".localtime()."\n";

        my $readcheck = sprintf "ADLER32:%08x", $crc;

        my $dstcheck = dCacheChecksum($outfile);
        if($dstcheck ne $readcheck) {
            my $msg = "Detected data corruption on write: dst checksum = $dstcheck != read checksum $readcheck,"
                . " writing \"$outfile\" on ".localtime()."  (try $numtries)\n";

            if($numtries < $maxtries) {
                warn $msg;
                # this check is racy, but still better than doing nothing
                die "The input file \"$infile\" is gone - stop now.\n" unless -r $infile;
                print localtime().": Removing file $outfile\n";
                unlink $outfile or die "Error unlinking \"$outfile\" on ".localtime().": $!\n";
                next COPYTRY;
            }
            else {
                die $msg;
            }
        }

        if(abs_path($infile) =~ m|^/pnfs|) {
            my $srccheck = dCacheChecksum($infile);
            die "Detected data corruption on read: read checksum = $readcheck != source checksum $srccheck\n"
                if($srccheck ne $readcheck);
        }

        # the copy has succeeded
        if($sha256) {
            $$sha256 = $dig->hexdigest;
        }

        return $readcheck;
    }
}

#================================================================
# Copy $infile to an $outfile on /pnfs using "ifdh cp".
# Die if dCache style adler32 checksum of the destination file
# does not match that of the source.
#
# A pre-requisite: the destination directory must exist.
#
sub ifdh_copy {
    my ($infile, $outfile, $inopts) = @_;


    my $srccheck = adler32Checksum($infile);

    my @args = ('ifdh', 'cp', $infile, $outfile);
    system(@args) == 0
        or die "ifdh_copy: system @args failed: $?\n";

    my $dstcheck = adler32Checksum($outfile);

    die "Detected data corruption during 'ifdh cp': dst checksum $dstcheck != source checksum $srccheck\n"
        if($srccheck ne $dstcheck);

    return $dstcheck;
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
