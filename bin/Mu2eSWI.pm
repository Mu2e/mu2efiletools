#!/usr/bin/perl -w
#
# Code shared by the different frontend scripts
#
# A.Gaponenko, 2016
#

package Mu2eSWI;
use strict;
use Exporter qw( import );
use File::Basename;
use LWP::UserAgent;
use Data::Dumper;

use Class::Struct Mu2eSWIFields =>
    { delay=>'$',
      maxtries=>'$',
      read_server=>'$',
      write_server=>'$',
    };

use base qw(Mu2eSWIFields);


sub optSpec {
    return [
        "delay=i",
        "maxtries=i",
        "read-server=s",
        "write-server=s",
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
    my $read_server = $df->read_server;
    my $write_server = $df->write_server;
    my $res = <<EOF
--read-server    Samweb server for querying. The default is
                 $read_server

--write-server   Samweb server to write file locations. The default is
                 $write_server

--maxtries=<int> Give up after the given number of failures
                 to post a request to the server. The default
                 is $maxtries.

--delay=<int>    Delay, in seconds, before retry on the first failure.
                 The default is $delay seconds.
                 The subsequent delays are randomly increased.
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
    return $self->{'Mu2eSWI::_ua'};
}

sub new {
    my ($class) = @_;

    my $self = $class->SUPER::new(
        delay=>60,
        maxtries=>3,
        read_server=>'http://samweb.fnal.gov:8480',
        write_server=>'https://samweb.fnal.gov:8483',
        );

    $self->{'Mu2eSWI::_ua'} = LWP::UserAgent->new(keep_alive=>1, agent=>_agentID);
    $self->ua->conn_cache->total_capacity(5);

    return $self;
}

#================================================================
1;
