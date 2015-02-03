package database;
use strict;
use warnings;
use DBI;
sub new{ # new database connection.  use like this: my $dbh = database->new( dbFile => $dbFile ,tables => { table1 => [qw: column1 column2 columnN ] } );
	my ($class,%args) = @_;
	my $self = bless {}, $class;
	$self->{dbFile} = delete $args{dbFile};
	print STDERR "sub database->new requires dbFile argument\n" and exit unless $self->{dbFile};
	$self->{dbh} = DBI->connect( "dbi:SQLite:dbname=$self->{dbFile}","","", { RaiseError => 1, PrintError => 1, sqlite_see_if_its_a_number => 1 } ) or die $!;
	# setup and optimize the sqlite database...
	$self->{dbh}->do("PRAGMA foreign_keys = ON");
	$self->{dbh}->do("PRAGMA synchronous = OFF");
	$self->{dbh}->do("PRAGMA cache_size = 1800000000");
	return $self;
}
sub schema{ # capture the database schema and return in a hash
	my $self = shift;
	my %schemaInfo;
	for my $schemaRef(map{$_}$self->sql(qq[select * from sqlite_master;]),){
		my %schema = %$schemaRef;
		map{$schema{$_}=defined $schema{$_}?$schema{$_}:''}keys %schema; # replacing undef with '' prevents warning
		my $name = delete $schema{name};
		my $type = delete $schema{type};
		if($type eq 'index'){
			my $tableName = delete $schema{tbl_name};
			map{$schemaInfo{$tableName}{$type}{$name}{$_}=$schemaInfo{$tableName}{$type}{$name}{$schema{$_}}} keys %schema; # coerce the index info into the hash
			map{$schemaInfo{$tableName}{$type}{$name}{$_->{cid}}=\%$_}$self->sql(q[PRAGMA index_info('].$name.q[');]); # coerce the index info into the hash
		}else{
			delete $schema{tbl_name};
			map{$schemaInfo{$name}{$_}=$schema{$_}}keys %schema; # coerce the table info into the hash
			map{$schemaInfo{$name}{column}{$_->{cid}}=\%$_}$self->sql(q[PRAGMA table_info('].$name.q[');]);  # coerce the table info into the hash
		}
		
	}
	return %schemaInfo;
}
sub sql{ # issues sql to database and returns data if any is returned from the database
	my $self = shift;
	my $stmt = shift;
	#print "sql: $stmt\n";  # uncomment to view sql as issued to database
	my $sth = $self->{dbh}->prepare($stmt);
	my ( %row, @rows );
	return unless $sth->execute; # return if no data is returned by the query
	if($sth->{NUM_OF_FIELDS}){ # build the array of rows returned from the database...
		$sth->bind_columns( \( @row{@{ $sth->{NAME} }} ));
		my $i = -1;
		while ( $sth->fetch ) {
			++$i;
			$rows[$i] = {};
			for ( keys %row){
				$rows[$i]{$_} = $row{$_};
			}
		}
		return @rows;
	}
}
sub tableNames{ # return all the table names in the database
	my $self = shift;
	my %dbSchema = $self->schema();
	return keys %dbSchema;
}
sub columnNames{ # return all the column names from a given table name
	my $self = shift;
	my $tableName = shift;
	my %dbSchema = $self->schema();
	return map{$dbSchema{$tableName}{column}{$_}{name}}sort keys %{$dbSchema{$tableName}{column}};
}
sub report{ # print a report displaying all the data in the database
	my $self = shift;
	for my $table ($self->tableNames()){ # gather all the table names
		print "table: $table\n";
		my @rows = $self->sql(qq{SELECT * FROM $table ORDER BY _fileLineNumber_ DESC;}); # select all the data from each table
		for my $row(@rows){ 
			for my $column ($self->columnNames($table)){ # gather all the column names
				print "  $column: $row->{$column}\n"; # print the data
			}
			print "\n";
		}
	}
}



package csvHandler;	# a class for capturing the data and reporting
use strict;
use warnings;
sub new{ # initialize the csvHandler object
	my ($class,%args) = @_;
	my $self = bless {}, $class;
	$self->{csvFile} = csvFile->new(fileName=>$args{fileName})->read()->parse(); # create a new csvFile object; read the file; parse the file contents into objects
	return $self;
}
sub report{ # print a report of the data captured from the input csv file
	my $self = shift;
	for my $line ($self->{csvFile}->lines()){ # iterate over the parsed lines from the csv file
		print 'line ', $line->number(),"\n";
		for my $field($line->fields()){ # iterate over the fields of the line
			print '  field ',$field->number(),': ', $field->value(), "\n";
		}
		print "\n";
	}
	return $self;
}
sub save{ # save the parsed data into the database
	my ($self,%args) = @_;
	my $tableName = $self->{csvFile}->{fileName}; # build the table name from the input file name
	$tableName =~ s/(?:\s+)|(?:\..*$)//g; # remove any spaces in the file name and remove the extension (like ".csv")
	my $sqh = database->new(dbFile => $args{dbFile});
	# work with the database...
	my $dbh = $sqh->{dbh}; # makes the $sqh->{dbh}->quote() method easier to read (below)
	{	# begin the save transaction...
		$sqh->sql(qq{SAVEPOINT insertTransaction;}); # a SQLite named transaction
		{	# build and execute the create table command...
			my $columns = join ", ", '_fileLineNumber_ integer unique primary key', @{$self->{csvFile}->fieldNames()}; # build the columns string
			$sqh->sql( qq{CREATE TABLE IF NOT EXISTS '$tableName'($columns);} ); # execute the command
		}
		{	# build and execute the insert command...
			for my $line ($self->{csvFile}->lines()){ # iterate over all the parsed lines from the csv file
				my $insertValues = join q{,}, $dbh->quote($line->number()), map{ $dbh->quote($_->value()) } $line->fields(); # build a string like 'value0', 'value1', 'value2'
				$sqh->sql(q{INSERT OR REPLACE INTO }.$tableName.q{ VALUES(}.$insertValues.q{);}); # save the data to the database; replace it if the line numbers are equal
			}
		}
		$sqh->sql(qq{RELEASE insertTransaction;}); # finish the SQLite named transaction
	}
	return $self;
}
sub load{ # load data from the database and return it as a hash
	my ($self,%args) = @_;
	
}



package csvFile; # a class for opening, reading, and parsing the csv file
use strict;
use warnings;
sub new{ # initialize the csvFile object
	my ($class,%args) = @_;
	my $self = bless {}, $class;
	$self->{fileName} = $args{fileName};
	return $self;
}
sub fieldNames{ # assign the names of the fields
	my $self = shift;
	return $self->{fieldNames};
}
sub lines{ # returns the lines of the csvFile as an array
	my $self = shift;
	return wantarray ? @{$self->{lines}} : join "$/", @{$self->{lines}};  # return either array or scalar depending on context
}
sub read{ # reads the file into an array 
	my $self = shift;
	open(my $fileHandle,'<',$self->{fileName});
	$self->{rawData} = ''; # initialize the rawData attribute
	while(<$fileHandle>){ # first seperate the field identification line from the data lines...
		if($.==1){ # first line of the file contains just field names
			chomp; # get rid of the line ending 
			@{$self->{fieldNames}} = split /,/,$_;
			$self->{nFields} = scalar @{$self->{fieldNames}};
		}else{ # collect all the data fields into a single scalar
			$self->{rawData} .= $_;
		}
	}
	close $fileHandle;
	return $self;
}
sub parse{ # parse the input csv file and populate @{$self->{lines}} attribute with csvLine objects
	my $self = shift;
	my @fieldData = split /(?:\A|,)("(?:[^"]+|"")*"|[^,]*)/m, $self->{rawData}; # divide all the csv fields into an array
	my $i=-1;
	my $nLine = 0;
	my $nField = 0;
	my @lines;
	@{$self->{lines}} = (csvLine->new(number=>$nLine));
	while(@fieldData){ # process the array of field data
			++$i;
			my $field = shift @fieldData;
			$field = '""' unless defined $field;	# explicitly set blank fields to an empty string
			if($i%2 == 0){ # if $i is even there is no field data, so skip it 
				next;
			}
			if($field =~ s/^"// and $field =~ s/"$//){ # if there are double quotes at the begining and end of the field remove them
				$field =~ s/""/"/g if $field; # remove two double quotes in a row from double-quoted fields
			}
			if($i == $self->{nFields}*2-1){ # only oddly numbered lines have data and numbering begins at zero
				my @fields = split /[\r\n]+/, $field; # the last field and the first field are seperated by a line ending.  these are split here
				$field = $fields[0]; # change the field we are working with
				unshift @fieldData,$fields[1] if defined $fields[1] and length $fields[1] > 0; # put $fields[1] back onto the array we are processing
				$self->{lines}[$nLine]->addField(value=>$field, number=>$nField); # capture the $field as a csvField object
				$i = 0;
				push @{$self->{lines}}, csvLine->new(number=>++$nLine) if scalar @fieldData>0; # don't make a new csvLine object if there is no more data
				$nField = 0;
				next;
				}
			if(defined $field){
				$self->{lines}[$nLine]->addField(value=>$field, number=>$nField); # capture the $field as a csvField object
				++$nField;
			}
	}
	return $self;
}



package csvData; # a parent class for csvLine and csvField objects ( these are defined below )
use strict;
use warnings;
sub new{ # initialize the csvLine object
	my ($class,%args) = @_;
	my $self = bless {}, $class;
	$self->{number} = $args{number}; # set the line number
	return $self;
}
sub number{ # return the number attribute
	my $self = shift;
	return $self->{number};
}



package csvLine; # a line from the csv parsing
use strict;
use warnings;
use base qw{ csvData };
sub new{ # initialize the csvLine object
	my ($class,%args) = @_;
	my $self = csvData->new(%args);
	bless $self, $class;
	$self->{number} = $args{number}; # set the line number
	@{$self->{fields}} = (); # initialize fields attribute
	return $self;
}
sub fields{
	my $self = shift;
	return wantarray ? @{$self->{fields}} : join ";", @{$self->{fields}};  # return either array or scalar depending on context
}

sub addField{
	my ($self,%args) = @_;
	push @{$self->{fields}}, csvField->new(value=>$args{value}, number=>$args{number});
}



package csvField; # a field from the csv parsing
use strict;
use warnings;
use base qw{ csvData };
sub new{ # initialize the csvField object
	my ($class,%args) = @_;
	my $self = csvData->new(%args);
	bless $self, $class;
	$self->{number} = $args{number}; # set the line number
	$self->{value} = $args{value}; # set the line value
	return $self;
}
sub value{
	my $self = shift;
	return $self->{value};
}
1;

