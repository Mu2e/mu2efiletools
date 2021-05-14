#!/usr/bin/perl -w
#
# Code shared by the different frontend scripts
#
# A.Gaponenko, 2016
#

package Mu2eSWI;
use strict;
use Exporter qw( import );
use English '-no_match_vars';
use Carp;
use File::Basename;
use LWP::UserAgent;
use HTTP::Request;
use HTTP::Request::Common;
use HTTP::Status qw(:constants);
use JSON;
use Time::HiRes qw(gettimeofday tv_interval);
use Data::Dumper;

use Class::Struct Mu2eSWIFields =>
    { delay=>'$',
      maxtries=>'$',
      samweb_server=>'$',
      timeout=>'$',
    };

use base qw(Mu2eSWIFields);


sub optSpec {
    return [
        "delay=i",
        "maxtries=i",
        "samweb_server=s",
        "timeout=i",
        ];
}

sub optDefaults {
    my ($self) = @_;
    my $res = {};
    my $s = Mu2eSWIFields->new; # do not use $self: skip ua etc.
    for my $long (keys %$s) {
        my $short = $long;
        $short =~ s/^.*:://;
        $res->{$short} = \$self->{$long};
    }
    return $res;
}

sub optDocString {
    my $self = shift;
    my $prefix = $_[0] // '';
    my $interspace = $_[1] // '';

    my $df = Mu2eSWI->new;
    my $delay = $df->delay;
    my $maxtries = $df->maxtries;
    my $samweb_server = $df->samweb_server;
    my $timeout = $df->timeout;
    my $res = <<EOF
--samweb_server  The samweb server to use. The default is
                 $samweb_server

                 If you intend to modify SAM database, run kx509
                 to generate authentication files before running the
                 script. Or set environment variables HTTPS_CERT_FILE
                 and HTTPS_KEY_FILE to point to appropriate files.

--maxtries=<int> Give up after the given number of failures
                 when sending a request to the server. The default
                 is $maxtries.

--delay=<int>    Delay, in seconds, before retry on the first failure.
                 The default is $delay seconds.
                 The subsequent delays are randomly increased.

--timeout=<int>  A SAMWEB request will be aborted if no activity on
                 the connection to the server is observed for
                 "timeout" seconds.  The default is $timeout.

EOF
;
    $res =~ s/^(.+)$/$prefix$1/mg;
    $res =~ s/^(\s*)(--[^\s]+)(\s*)/$1$2$3$interspace/mg;
    $res =~ s/^(\s*)([^\s-])/$1$interspace$2/mg;
    return $res;
}

sub _agentID() {
    # Production versions have the *_DIR enviroment variable set by UPS.
    my $ver = $ENV{'MU2EFILETOOLS_DIR'} // 'dev';
    $ver = basename($ver);
    my $app = basename($0);
    return "$app/$ver";
}

sub ua {
    my ($self) = @_;
    $self->{'Mu2eSWI::_ua'}->timeout($self->timeout);
    return $self->{'Mu2eSWI::_ua'};
}

sub new {
    my ($class) = @_;

    my $self = $class->SUPER::new(
        delay=>60,
        maxtries=>3,
        samweb_server=>'https://samweb.fnal.gov:8483',
        timeout => 3600,
        );

    $self->{'Mu2eSWI::_ua'} = LWP::UserAgent->new(keep_alive=>1,
                                                  agent=>_agentID,
                                                  timeout=>$self->timeout,
        );
    $self->{'Mu2eSWI::_ua'} ->conn_cache->total_capacity(5);

    return $self;
}

#================================================================
sub authConfig {
    my ($self) = @_;

    my $numvars = (defined $ENV{'HTTPS_CERT_FILE'} ? 1 : 0)
        + (defined $ENV{'HTTPS_KEY_FILE'} ? 1 : 0);

    if($numvars == 1) {
        croak "Error: either both or none of HTTPS_CERT_FILE, HTTPS_KEY_FILE environment variables should be defined.\n";
    }
    elsif($numvars == 2) {
        for my $name ('HTTPS_CERT_FILE', 'HTTPS_KEY_FILE') {
            croak "Error: the file $ENV{$name} specified  by environment variable $name is not readable.\n"
                unless -r $ENV{$name};
        }
    }

    my $filename = undef;
    if($numvars == 0) {
        $filename = '/tmp/x509up_u'.$EUID;
        croak "Error: no HTTPS_KEY_FILE and HTTPS_CERT_FILE in the environment, and file $filename does not exist.\n"
            ."Run kx509 and try again.\n"
            unless -r $filename;
    }

    $self->{'Mu2eSWI::_ua'} ->ssl_opts(
        SSL_cert_file => $ENV{'HTTPS_CERT_FILE'} // $filename,
        SSL_key_file => $ENV{'HTTPS_KEY_FILE'} // $filename,
        SSL_ca_path =>  $ENV{'HTTPS_CA_DIR'}  // '/etc/grid-security/certificates',
        # SSL_ca_file => $ENV{'HTTPS_CA_FILE'} // $filename,
        );


    #print "New ssl_opts: ", Dumper($self->{'Mu2eSWI::_ua'} ->ssl_opts), "\n";
    return 1;
}

#================================================================
sub testSSLConnection {
    my ($self, $inopts) = @_;
    my $opts = $inopts // {};
    my $verbosity = $$opts{'verbosity'} // 0;

    my $url = URI->new( $self->samweb_server.'/sam/mu2e/api' );
    $url->query_form( 'format' => 'plain');
    my $res = $self->ua->get($url);
    if($res->is_success) {
        print $res->content, "\n" if $verbosity;
        return 1;
    }
    else {
        print STDERR "Dump of the server response:\n", Dumper($res),"\n\n";
        return 0;
    }
}

#================================================================
# The last argument is a reference to a hash.  The call behavior is
# affected by the following keys, all of which are optional:
#
# verbosity         : the verbosity level. Default is 1.
#
# serverDeclareTime : the value will be treated as a ref to a scalar,
#                     and the scalar will be incremented by the time
#                     spent talking to the server.
#
# noRetryCodes      : a reference to list of HTTP failure codes on
#                     which the request is not retried. See
#                     the code below for the default list.
#
# allowedFailCodes  : a reference to list of HTTP failure codes on
#                     which to return failure instead of retrying or
#                     croaking.  If the list is empty (or not
#                     defined), the method either succeeds or croaks.
#
# The return value is the server response (from the last re-try, if any).

sub declareFile {
    my ($self, $jstext, $inopts) = @_;
    my $opts = $inopts // {};
    my $timing = $$opts{'serverDeclareTime'};

    my $verbosity = $$opts{'verbosity'} // 1;
    my $allowedFailCodes = $$opts{'allowedFailCodes'} // [];
    my $noRetryCodes = $$opts{'noRetryCodes'} //
        [ HTTP_CONFLICT, # file already declared
          HTTP_BAD_REQUEST, # bad metadata, e.g. unknown parent
        ];

    $self->ensureDatasetDefinition($jstext, $inopts);

    # Create a request
    my $req = HTTP::Request->new(POST => $self->samweb_server . '/sam/mu2e/api/files');
    $req->content_type('application/json');
    $req->content($jstext);

    my $numtries = 0;
    my $delay = $self->delay;

    while(1) {

        ++$numtries;

        # Measure the timing
        my $t1 = $timing ? [gettimeofday()] : undef;

        # Pass request to the user agent and get a response back
        my $res = $self->ua->request($req);

        if($timing) {
            my $elapsed = tv_interval($t1);
            $$timing += $elapsed;
        }

        # Check the outcome of the response
        if ($res->is_success) {
            return $res;
        }
        else {
            # allowedFailCodes lists "failures" expected in the normal
            # operation such as during the check for duplicate jobs.
            # Return before the "debug" printouts below.
            for my $code (@$allowedFailCodes) {
                return $res if($code == $res->code);
            }

            my $now_string = localtime();

            for my $code (@$noRetryCodes) {
                if($code == $res->code) {
                    croak "Error: got server response ",
                    $res->status_line, ".\n",
                    $res->content, "\n",
                    "Stopping on $now_string.";
                }
            }

            print STDERR "Got ",$res->status_line," on $now_string\n" if $verbosity > 0;
            print STDERR "Dump of the server response:\n", Dumper($res), "\n" if $verbosity > 1;

            if($numtries >= $self->maxtries) {
                croak "Error: $numtries tries failed. Stopping on $now_string due to: ",
                $res->status_line, ".\n", $res->content,"\n";
            }
            else {
                print STDERR "Will retry in $delay seconds\n" if $verbosity > 0;
                sleep $delay;
                $delay += int(rand($delay));
            }
        }
    }
}

#================================================================
sub listFiles {
    my ($self, $samquery, $inopts) = @_;
    my $opts = $inopts // {};
    my $verbosity = $$opts{'verbosity'} // 1;

    my $url = URI->new( $self->samweb_server.'/sam/mu2e/api/files/list' );
    $url->query_form( 'format' => 'plain', 'dims' => $samquery );

    my $res = $self->ua->get($url);
    if($res->is_success) {
        my @fns = split(/\n/, $res->content);
        chomp(@fns);
        return sort @fns;
    }
    else {
        print STDERR "Dump of the server response:\n", Dumper($res),"\n\n"
            if $verbosity > 1;

        croak "Error: got server response ",
        $res->status_line, ".\n",
        $res->content, "\n",
        "Stopping on ",
        scalar(localtime()),
        ".\n";
    }
}

#================================================================
# SAM allows to  restrict the listing by user.
# However in the Mu2e case dataset "owned" by some users
# were registerd in SAM by mu2epro, so a server-side
# select will not do the right thing.  We have to
# get all the definitions and filter them by hand.
sub listDefinitions {
    my ($self, $inopts) = @_;
    my $opts = $inopts // {};
    my $verbosity = $$opts{'verbosity'} // 1;

    my $url = URI->new( $self->samweb_server.'/sam/mu2e/api/definitions/list' );
    $url->query_form( 'format' => 'plain');
    my $res = $self->ua->get($url);
    if($res->is_success) {
        my @fns = split(/\n/, $res->content);
        chomp(@fns);
        return sort @fns;
    }
    else {
        print STDERR "Dump of the server response:\n", Dumper($res),"\n\n"
            if $verbosity > 1;

        croak "Error: got server response ",
        $res->status_line, ".\n",
        $res->content, "\n",
        "Stopping on ",
        scalar(localtime()),
        ".\n";
    }
}

#================================================================
sub describeDatasetDefinition {
    my ($self, $dsname) = @_;
    my $url = URI->new( $self->samweb_server.'/sam/mu2e/api/definitions/name/'.$dsname.'/describe' );
    $url->query_form( 'format' => 'plain' );
    my $res = $self->ua->get($url);
    return $res;
}

#================================================================
my %knownDatasets;
sub ensureDatasetDefinition {
    my ($self, $jstext, $inopts) = @_;
    my $opts = $inopts // {};
    my $verbosity = $$opts{'verbosity'} // 1;

    my $jsp = from_json($jstext);

    my $dsname = $jsp->{'dh.dataset'} // '';

    return if $dsname eq '';

    return if $knownDatasets{$dsname}//0;

    # does SAM already know about this dataset?
    my $dsdef = $self->describeDatasetDefinition($dsname);
    if($dsdef->is_success) {
        $knownDatasets{$dsname} = 1;
        return;
    }

    die "Error checking dataset definition for $dsname: got server response", Dumper($dsdef), "\n"
        unless ($dsdef->code == HTTP_NOT_FOUND);

    print "Creating a dataset definition for $dsname\n"
        if $verbosity > 0;

    # We need to create a definition for $dsname
    my $res = $self->ua->request(
        POST $self->samweb_server.'/sam/mu2e/api/definitions/create',
        Content => {
            'name' => $dsname,
            'dims'=>"dh.dataset=$dsname",
            'group'=>'mu2e',
        });

    die "Error creating dataset definition for $dsname: ",
    $res->status_line, ".\n",
    $res->content, "\n",
    "Stopping on ",
    scalar(localtime()),
    ".\n"
        unless $res->is_success;

    $knownDatasets{$dsname} = 1;
}

#================================================================
# The last argument is a reference to a hash.  The call behavior is
# affected by the following keys, all of which are optional:
#
# verbosity         : the verbosity level. Default is 1.
#
# metadataQueryTime : the value will be treated as a ref to a scalar,
#                     and the scalar will be incremented by the time
#                     spent talking to the server.
#
# allowedFailCodes  : a reference to list of HTTP failure codes on
#                     which to return failure instead of retrying or
#                     croaking.  If the list is empty (or not
#                     defined), the method either succeeds or croaks.
#
# noRetryCodes      : a reference to list of HTTP failure codes on
#                     which the request is not retried.
#
# The return value is a JSON struct, or undef (if allowedFailCodes
# is non-empty).

sub getFileMetadata {
    my ($self,$filename, $inopts) = @_;

    my $opts = $inopts // {};
    my $verbosity = $$opts{'verbosity'} // 1;
    my $timing = $$opts{'metadataQueryTime'};
    my $allowedFailCodes = $$opts{'allowedFailCodes'} // [];
    my $noRetryCodes = $$opts{'noRetryCodes'} // [];

    my $req = HTTP::Request->new(GET => $self->samweb_server.'/sam/mu2e/api/files/name/'.$filename.'/metadata?format=json');

    my $numtries = 0;
    my $delay = $self->delay;
    while(1) {
        ++$numtries;

        # Measure the timing
        my $t1 = $timing ? [gettimeofday()] : undef;

        my $res = $self->ua->request($req);

        if($timing) {
            my $elapsed = tv_interval($t1);
            $$timing += $elapsed;
        }

        if ($res->is_success) {
            my $jstext = $res->content;
            print "got json = $jstext\n" if $verbosity > 8;
            return from_json($jstext);
        }
        else {
            # allowedFailCodes lists "failures" expected in the normal
            # operation such as when checking whether a record for a
            # file is present.
            # Return before the "debug" printouts below.
            for my $code (@$allowedFailCodes) {
                return undef if($code == $res->code);
            }

            my $now_string = localtime();
            for my $code (@$noRetryCodes) {
                if($code == $res->code) {
                    croak "Error: got server response ",
                    $res->status_line, ".\n",
                    $res->content, "\n",
                    "Stopping on $now_string.";
                }
            }

            print STDERR "Error querying metadata for file $filename: ",$res->status_line," on $now_string\n" if $verbosity > 0;
            print STDERR "Dump of the server response:\n", Dumper($res), "\n" if $verbosity > 8;
            if($numtries >= $self->maxtries) {
                die "Error: $numtries tries failed. Stopping on $now_string due to: ",
                $res->status_line, ".\n", $res->content,"\n";
            }
            else {
                print STDERR "Will retry in $delay seconds\n" if $verbosity > 0;
                sleep $delay;
                $delay += int(rand($delay));
            }
        }
    }
}

#================================================================
sub getSamSha {
    my ($self,$filename, $inopts) = @_;

    my $js = $self->getFileMetadata($filename, $inopts);
    croak "Error getting metadata for $filename on ".localtime()."\n"
        unless defined($js);

    my $jssha = $js->{'dh.sha256'};
    croak "Error: no dh.sha256 SAM record for file $filename on ".localtime()."\n"
        unless defined $jssha;

    return $jssha;
}

#================================================================
# The last argument is a reference to a hash.  The call behavior is
# affected by the following keys, all of which are optional:
#
# dryrun            : Do not modify the database if set. The default is 0.
#
# verbosity         : the verbosity level. The default is 1.
#
# serverUpdateTime  : the value will be treated as a ref to a scalar,
#                     and the scalar will be incremented by the time
#                     spent talking to the server.
#
# Returns the number of added locations: 0 if dryrun is used, 1 otherwise.
#
sub maybeAddLocationToSam {
    my ($self, $fn, $location, $inopts) = @_;

    my $opts = $inopts // {};
    my $verbosity = $$opts{'verbosity'} // 1;
    my $dryrun = $$opts{'dryrun'} // 0;
    my $timing = $$opts{'serverUpdateTime'};

    # Note: the documentation at
    # https://cdcvs.fnal.gov/redmine/projects/sam-web/wiki/Interface_definitions
    # specifies HTTP PUT for adding a location. However it seems we should use POST instead.

    print +($dryrun ? "Would add" : "Adding" ). " to SAM location = $location\n" if $verbosity > 8;
    if(not $dryrun) {

        my $numtries = 0;
        my $delay = $self->delay;
        while(1) {
            ++$numtries;

            my $t1 = $timing ? [gettimeofday()] : undef;
            my $res = $self->ua->request(
                POST $self->samweb_server.'/sam/mu2e/api/files/name/'.$fn.'/locations',
                Content => {'add' => $location }
                );

            if($timing) {
                my $elapsed = tv_interval($t1);
                $$timing += $elapsed;
            }

            if ($res->is_success) {
                print "Added location $location for file $fn to SAM. Server response:\n", Dumper($res), "\n" if $verbosity > 8;
                return 1;
            }
            else {
                print STDERR "Error adding location $location for for file $fn: ",$res->status_line," on ".localtime()."\n" if $verbosity > 0;
                print STDERR "Dump of the server response:\n", Dumper($res), "\n" if $verbosity > 8;

                if($numtries >= $self->maxtries) {
                    die "Error: $numtries tries failed. Stopping on ".localtime()." due to: ",
                    $res->status_line, ".\n", $res->content,"\n";
                }
                else {
                    print STDERR "Will retry in $delay seconds\n" if $verbosity > 0;
                    sleep $delay;
                    $delay += int(rand($delay));
                }
            }

        } # retry loop
    } # not(dryrun)

    return 0;
}

#================================================================
1;
