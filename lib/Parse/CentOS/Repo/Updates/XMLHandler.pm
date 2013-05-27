package Parse::CentOS::Repo::Updates::XMLHandler;
# ABSTRACT: parse the repodata/repomd.xml

use base qw(XML::SAX::Base);

use strict;
use warnings;

my $current;
my $timestamp;
my %meta;

sub start_element {
    my ( $self, $el ) = @_;

    if ( $el->{Name} eq 'data' ) {
        return unless $el->{Attributes}{'{}type'};
        return unless $el->{Attributes}{'{}type'}{Value};
        return
            unless $el->{Attributes}{'{}type'}{Value}
            =~ m/^(filelists_db|primary_db|other_db)$/;

        $current = $el->{Attributes}{'{}type'}{Value};
        $current =~ s/_db$//;
    }
    else {
        return unless $current;

        if ( $el->{Name} eq 'location' ) {
            return unless $el->{Attributes}{'{}href'};
            return unless $el->{Attributes}{'{}href'}{Value};

            $meta{$current}{url} = $el->{Attributes}{'{}href'}{Value};
        }
        elsif ( $el->{Name} eq 'timestamp' ) {
            ++$timestamp;
        }
    }
}

sub end_element {
    my ( $self, $el ) = @_;

    if ( $el->{Name} eq 'data' ) {
        undef $current;
    }
    elsif ( $el->{Name} eq 'timestamp' ) {
        undef $timestamp;
    }
}

sub characters {
    my ( $self, $el ) = @_;

    if ($timestamp) {
        return if $el->{Data} =~ m/^\s*$/;
        $meta{$current}{epoch} = int $el->{Data};
    }
}

sub meta { \%meta }

1;
__END__
