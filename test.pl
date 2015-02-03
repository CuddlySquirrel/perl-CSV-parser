#!/usr/bin/perl
use strict;
use warnings;
use Term::ANSIColor qw{:constants}; # color in the terminal
use codeSample; # this is my codeSample.pm file with all the OO perl objects
# this code uses DBD::SQLite available here: http://search.cpan.org/~adamk/DBD-SQLite-1.37/lib/DBD/SQLite.pm

my $dbFile = 'test.sqlite'; # name the database file
my $autosFile = 'autos.csv'; # autos csv file
my $bookReviewsFile = 'bookReviews.csv'; # book reviews csv file


{	# First we work with the autos.csv file...
	my $autos = csvHandler->new(fileName=>$autosFile); # create a new csvHandler object

	print "\n\n\ndumping the $autosFile csvHandler object...\n\n";
	dumphash( %{$autos} );  # report the entire contents of the csvHandler object

	print "\n\n\ngenerate report from the $autosFile csvHandler object...\n\n";
	$autos->report(); # report autos data from csv file

	$autos->save(dbFile=>$dbFile); # save the autos data to the database
}

print "\n\n",' -'x40,"\n\n\n"; # make a nice looking seperator in our report

{	# Second we work with the bookReviews.csv data...
	my $bookReviews = csvHandler->new(fileName=>$bookReviewsFile); # create a new csvHandler object
	
	$bookReviews->save(dbFile=>$dbFile); # save the autos data to the database

	# Third we generate a database report...
	my $dbh = database->new(dbFile=>$dbFile);  # build database connection
	print "\n\n\nNow we generate a report from the database.  If we were successful this report should show every field from each record from all the files, and all the fields should be labeled consistently with the first line of the csv file they came from...\n\n\n";
	$dbh->report(); # the report displays the data with the _fileLineNumber_ values in descending order to prove that is where the data actually is
}




sub dumphash { 
	no warnings;
	my %obj = my %hash = @_;
	my $caller = caller;
	$main::depth++;
	$main::n_keys = ( defined $main::n_keys ) ? $main::n_keys :  keys %hash;
	my $n_keys = keys %hash;
	my $n = 0;
	for my $key ( sort keys %hash ){
		$n++ if $main::depth == 1;
		my $value = $hash{$key};
		my ( $keytype, $valuetype ) = ( ref $key, ref $value );
		if ( $valuetype eq 'HASH' ){
			msg( ". "x($main::depth-1) );
			msg( "\\E\\Ohsh\\R $key\n" );
			dumphash( %$value );
			$main::depth--;
		}elsif ( $valuetype eq 'ARRAY' or $key =~ /\AARRAY\(/ ){
			msg( ". "x($main::depth-1) );
			msg( "\\Carr\\R $key\n" );
			my $i = -1; my $is;
			my %h = map{ eval{ $is = ++$i; while ( length $is < length $#$value ){ $is = ' '.$is; }return '['.$is.']'}, $_ } @$value; 
			dumphash( %h );
			$main::depth--;
		}elsif ( $value =~ /\w+=HASH/ ){
			msg( ". "x($main::depth-1) );
			msg( "\\Tobj\\R $key" );
			msg( "($valuetype) \n" )  if $valuetype;
			msg( "$value\n" )  if ! $valuetype;
			my %h = keys %$value;
			dumphash( %$value );
			$main::depth--;
		}elsif ( $key =~ /\w+=HASH/ ){
			msg( ". "x($main::depth-1) );
			msg( "\\Tobj\\R $key" );
			msg( "\n" );
			my %h = %obj;
			print join ', ', keys %h;
			$main::recursion++;
			dumphash( %h ) unless $main::recursion > 9;
			$main::depth--;
		}else{
			$value = 'undef' unless defined $value;
			msg( ". "x($main::depth-1) );
			msg( "\\Gstr\\R \\N$key\\R  $value\n" );
		}
	}
	if ( $n == $main::n_keys ){
		undef $main::depth;
		undef $main::n_keys;
	}
	
}
sub msg{
	my %args 	= scalar @_ == 1	? ( msg => shift )  	: @_	   ;  	# if only one argument sent, assign it to msg argkey
	my $message = $args{msg};
	return if !$message;
	$message =~ s/\\R/\e[0m/g;   ## reset
	$message =~ s/\\O/\e[1m/g;   ## bold
	$message =~ s/\\N/\e[4m/g;   ## underline
	$message =~ s/\\B/\e[30m/g;  ## black
	$message =~ s/\\E/\e[31m/g;  ## red
	$message =~ s/\\G/\e[32m/g;  ## green
	$message =~ s/\\Y/\e[33m/g;  ## yellow
	$message =~ s/\\T/\e[34m/g;  ## blue
	$message =~ s/\\C/\e[35m/g;  ## magenta
	$message =~ s/\\W/\e[36m/g;  ## cyan
	$message =~ s/\\W/\e[37m/g;  ## white
	print $message.RESET;
}