package Parse::CentOS::Repo::Updates;
# ABSTRACT: parse the data from a CentOS update repo

use strict;
use warnings;

use Moo;
use MooX::Types::MooseLike::Base qw( HashRef Str );

use Carp;
use DBIx::Lite;
use File::Basename;
use HTTP::Tiny;
use IO::Uncompress::Bunzip2 qw( bunzip2 $Bunzip2Error );
use Path::Tiny;
use XML::SAX;

use Parse::CentOS::Repo::Updates::XMLHandler;

my @PRIMARY_PACKAGES_COLUMNS = qw(
    pkgKey
    pkgId
    name
    arch
    version
    epoch
    release
    summary
    description
    url
    time_file
    time_build
    rpm_license
    rpm_vendor
    rpm_group
    rpm_buildhost
    rpm_sourcerpm
    rpm_header_start
    rpm_header_end
    rpm_packager
    size_package
    size_installed
    size_archive
    location_href
    location_base
    checksum_type
);

has major => (
    is  => 'ro',
    isa => Str,
);

has arch => (
    is  => 'ro',
    isa => Str,
);

has base_url => (
    is  => 'ro',
    isa => Str,
);

has cache_dir => (
    is  => 'ro',
    isa => Str,
);

# primary / other / filelists
has _meta_path => (
    is  => 'rw',
    isa => HashRef,
);

# primary / other / filelists
has _meta_url => (
    is  => 'rw',
    isa => HashRef,
);

sub _build_meta_url {
    my $self = shift;

    my $res = HTTP::Tiny->new->get( $self->_update_url . "/repodata/repomd.xml" );
    croak "cannot get repomd.xml\n" unless $res->{success};

    my $parser = XML::SAX::ParserFactory->parser(
        Handler => Parse::CentOS::Repo::Updates::XMLHandler->new,
    );
    $parser->parse_string( $res->{content} );

    my $meta = $parser->get_handler->meta;
    $self->_meta_url({ %$meta });
}

sub _build_meta_path {
    my $self = shift;

    croak "cache_dir is needed\n" unless $self->cache_dir;

    my $dir = path($self->cache_dir);
    $self->_meta_path({
        primary    => $dir->child('primary.sqlite.bz2'),
        other      => $dir->child('other.sqlite.bz2'),
        filelists  => $dir->child('filelists.sqlite.bz2'),
    });
}

sub BUILD {
    my $self = shift;

    $self->_build_meta_url  if $self->base_url && $self->major && $self->arch;
    $self->_build_meta_path if $self->cache_dir;

    my $dir = path( $self->cache_dir );
    $dir->mkpath unless $dir->exists;
}

sub _update_url {
    my $self = shift;

    croak "base_url is needed\n" unless $self->base_url;
    croak "major is needed\n"    unless $self->major;
    croak "arch is needed\n"     unless $self->arch;

    my $url = sprintf(
        '%s/%s/updates/%s',
        $self->base_url,
        $self->major,
        $self->arch,
    );

    return $url;
}

sub update {
    my ( $self, $force ) = @_;

    croak "base_url is needed\n" unless $self->base_url;

    for ( qw/ primary other filelists / ) {
        next unless $force || !$self->is_recent($_);

        my $file  = $self->_meta_path->{$_};
        my $url   = $self->_meta_url->{$_}{url};
        my $epoch = $self->_meta_url->{$_}{epoch};

        my $res = HTTP::Tiny->new->get( $self->_update_url . "/$url" );
        croak "cannot get $url\n" unless $res->{success};

        $file->spew_raw( $res->{content} );
        $file->touch($epoch) if $epoch;

        my $db = basename( $file, ".bz2" );
        bunzip2 $file->canonpath, $file->parent->child($db)->canonpath
            or warn("bunzip2 failed: $Bunzip2Error\n"), next;
    }
}

sub is_recent {
    my ( $self, $item ) = @_;

    croak "base_url is needed\n"  unless $self->base_url;
    croak "unknown item: $item\n" if $item && $item !~ m/^(primary|other|filelists)$/;

    my @items = qw( primary other filelists );
    @items = ( $item ) if $item;

    for (@items) {
        my $file  = $self->_meta_path->{$_};
        my $url   = $self->_meta_url->{$_}{url};
        my $epoch = $self->_meta_url->{$_}{epoch};

        return unless $file->exists;
        return if     $file->stat->mtime < $epoch;
    }

    return 1;
}

sub get_updates {
    my $self = shift;

    #
    # SELECT * FROM packages
    #
    my $result = do {
        my $db   = path($self->cache_dir)->child('primary.sqlite');
        my $dbix = DBIx::Lite->connect("dbi:SQLite:$db")
            or croak "cannot connect to $db\n";
        $dbix->schema->table('packages')->autopk('pkgKey');
        $dbix->schema->table('packages')->resultset_class('Parse::CentOS::Repo::Updates::ResultSet');

        my $rs
            = $dbix->table('packages')
            ->select(@PRIMARY_PACKAGES_COLUMNS)
            ->order_by('pkgKey')
            ;

        $rs;
    };

    return $result;
}

sub get_security_updates {
    my $self = shift;

    #
    # SELECT pkgKey
    # FROM changelog
    # WHERE
    #        changelog LIKE '%CVE%'
    #     OR changelog like '%security%'
    # GROUP BY pkgKey;
    #
    my @pkg_keys = do {
        my $db   = path($self->cache_dir)->child('other.sqlite');
        my $dbix = DBIx::Lite->connect("dbi:SQLite:$db")
            or croak "cannot connect to $db\n";

        my $rs
            = $dbix->table('changelog')
            ->select('pkgKey')
            ->search( { changelog => { -like => [ '%security%', '%CVE%' ] } }, )
            ->group_by('pkgKey')
            ;

        $rs->get_column('pkgKey');
    };

    #
    # SELECT *
    # FROM packages
    # WHERE pkgKey = ?;
    #
    my $result = do {
        my $db   = path($self->cache_dir)->child('primary.sqlite');
        my $dbix = DBIx::Lite->connect("dbi:SQLite:$db")
            or croak "cannot connect to $db\n";
        $dbix->schema->table('packages')->autopk('pkgKey');
        $dbix->schema->table('packages')->resultset_class('Parse::CentOS::Repo::Updates::ResultSet');

        my $rs
            = $dbix->table('packages')
            ->select(@PRIMARY_PACKAGES_COLUMNS)
            ->search( { pkgKey => \@pkg_keys }, )
            ->order_by('pkgKey')
            ;

        $rs;
    };

    return $result;
}

1;
__END__

=head1 SYNOPSIS

    use Parse::CentOS::Repo::Updates;
    ...


=head1 DESCRIPTION

...
