package BinarySearchTree;
use strict;
use warnings;
use parent 'Displayable';

use Data::Dumper qw(Dumper);
use Carp;

use BinarySearchTree::Node;
use POSIX;

sub new {
	my ($class, $sentinel, $min_valid_key) = @_;

	my $self = {
		root => BinarySearchTree::Node->new($sentinel, undef, DBL_MAX),
		min_valid_key => $min_valid_key,
	};

	bless $self, $class;
	return $self;
}

sub add_content {
	my ($self, $content) = @_;

	my $node = $self->{root}->find_node($content);
	confess "found duplicate for $content" if defined $node;

	return $self->{root}->add($content);
}

sub remove_content {
	my ($self, $content) = @_;

	my $node = $self->{root}->find_node($content);

	$node->remove();

	return;
}

sub remove_node {
	my ($node) = @_;
	return $node->remove();
}

sub find_content {
	my ($self, $key) = @_;

	my $node = $self->{root}->find_node($key);

	return $node->content() if defined $node;
	return;
}

sub nodes_loop {
	my ($self, $start_key, $end_key, $routine) = @_;

	$start_key = $self->{min_valid_key} unless defined $start_key;

	$self->{root}->nodes_loop($start_key, $end_key, $routine);

	return;
}

sub save_svg {
	my ($self, $filename) = @_;

	$self->{root}->save_svg($filename);

	return;
}

1;
