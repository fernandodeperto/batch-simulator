package BinarySearchTree;
use strict;
use warnings;
use parent 'Displayable';

use Data::Dumper qw(Dumper);
use Carp;

use BinarySearchTree::Node;
use POSIX;

sub new {
	my $class = shift;
	my $sentinel = shift;

	my $self = {
		root => BinarySearchTree::Node->new($sentinel, undef, DBL_MAX),
		min_valid_key => shift
	};

	bless $self, $class;
	return $self;
}

sub add_content {
	my $self = shift;
	my $content = shift;

	my $node = $self->{root}->find_node($content);

	confess "$content found in $node->{content}" if defined $node; # check to see if we are not inserting duplicated content

	return $self->{root}->add($content);
}

sub remove_content {
	my $self = shift;
	my $content = shift;

	my $node = $self->{root}->find_node($content);
	$node->remove();
	return;
}

sub display_statistics {
	my $self = shift;
	my ($height, $nodes_number) = $self->{root}->compute_statistics();
	my $min_height = log($nodes_number) / log(2);
	my $ratio = $height / $min_height;
	print STDERR "height is $height, we have $nodes_number nodes ; we need at least a hight $min_height but we are $ratio times more\n";
	return;
}

sub remove_node {
	my $node = shift;
	return $node->remove();
}

sub find_content {
	my $self = shift;
	my $key = shift;
	my $node = $self->{root}->find_node($key);
	return $node->content() if defined $node;
	return;
}

sub nodes_loop {
	my $self = shift;
	my $start_key = shift;
	my $end_key = shift;
	my $routine = shift;

	$start_key = $self->{min_valid_key} unless defined $start_key;

	$self->{root}->nodes_loop($start_key, $end_key, $routine);
	return;
}

sub save_svg {
	my $self = shift;
	my $filename = shift;
	$self->{root}->save_svg($filename);
	return;
}

1;
