set ansi_nulls on;
set ansi_padding on;
set ansi_warnings on;
set arithabort on;
set concat_null_yields_null on;
set quoted_identifier on;
set statistics io off;
set statistics time off;
go

create or alter procedure sp_Merge
	@SourceTempTableName sysname,
	@TargetSchemaName sysname,
	@TargetTableName sysname,
	@BatchSize int = 1000,
	@IsDeletedColumnName sysname = N'sp_Merge__RowIsDeleted',
	@Debug bit = 0
as
begin
	if @@trancount != 0 throw 50000, N'Open transaction not allowed.', 1;
	if isnull(has_perms_by_name(null, null, 'VIEW SERVER STATE'), 0) = 0 throw 50000, N'Permission required: VIEW SERVER STATE.', 1;

	declare @SourceObjectId int = object_id(concat('tempdb..', @SourceTempTableName), 'U');
	if @SourceObjectId is null throw 50000, N'Source table must exist.', 1;
	if @SourceObjectId > 0 throw 50000, N'Source table must be a temporary table (e.g.: #Temp), not a global temporary table (e.g.: ##Temp).', 1;
	if not exists (select 1 from tempdb.sys.indexes where object_id = @SourceObjectId and is_primary_key = 1) throw 50000, N'Source table must have a primary key.', 1;

	declare @TargetName nvarchar(261) = concat(quotename(@TargetSchemaName), '.', quotename(@TargetTableName));
	declare @TargetObjectId int = object_id(@TargetName, 'U');
	if @TargetObjectId is null throw 50000, N'@TargetSchemaName and @TargetTableName must refer to a table that exists.', 1;
	if not exists (select 1 from sys.indexes where object_id = @TargetObjectId and is_primary_key = 1) throw 50000, N'Target table must have a primary key.', 1;

	declare @Msg nvarchar(max);
	declare @Query nvarchar(max);

	/*
	COLUMN DETAILS
	*/
	/*
	create table #Table (
		DatabaseId int not null,
		DatabaseName sysname not null,
		TableId int not null,
		TableName nvarchar(300) not null,
		HasPrimaryKey bit not null default 0,
		primary key (TableId)
	);

	create table #Column (
		DatabaseId int not null,
		TableId int not null,
		ColumnId int not null,
		UserTypeId int not null,
		IsComputed bit not null,
		IsNonTargetColumn as isnull(convert(bit, case when type_id('timestamp') = UserTypeId or IsComputed = 1 then 1 else 0 end), 0),
		primary key (TableId, ColumnId)
	);

	select db_id() as DatabaseId, *
	into #TypeMetadata
	from sys.types t
	union all
	select db_id('tempdb'), *
	from tempdb.sys.types t;

	select db_id() as DatabaseId, *
	into #TableMetadata
	from sys.tables t
	where t.object_id = @TargetObjectId
	union all
	select db_id('tempdb'), *
	from tempdb.sys.tables t
	where t.object_id = @SourceObjectId;

	select *
	into #ColumnMetadata
	from sys.columns c
	where c.object_id = @TargetObjectId
	union all
	select *
	from tempdb.sys.columns c
	where c.object_id = @SourceObjectId;

	select *
	into #IndexMetadata
	from sys.indexes i
	where i.object_id = @TargetObjectId
	union all
	select *
	from tempdb.sys.indexes i
	where i.object_id = @SourceObjectId;

	select *
	into #IndexColumnMetadata
	from sys.index_columns ic
	where ic.object_id = @TargetObjectId
	union all
	select *
	from tempdb.sys.index_columns ic
	where ic.object_id = @SourceObjectId;

	select * from #TypeMetadata;
	select * from #TableMetadata;
	select * from #ColumnMetadata;
	select * from #IndexMetadata;
	select * from #IndexColumnMetadata;

	--insert into #Table (DatabaseId, TableId, TableSchema, TableName)
	--select DatabaseId, object_id, 
	--from #TableMetadata;

	--insert into #Column (TableId, ColumnId, UserTypeId, IsComputed)
	--select c.object_id, c.column_id, c.user_type_id, c.is_computed
	--from (
	--	select *
	--	from (
	--		select * from sys.columns c
	--		union all
	--		select * from tempdb.sys.columns c
	--	) c
	--	where c.object_id in (@TargetObjectId, @SourceObjectId)
	--) c;

	select * from #Table;
	select * from #Column;

	create table #TargetTableNonTargetColumn (
		TableId int not null,
		ColumnId int not null,
		primary key (TableId, ColumnId)
	);

	insert into #TargetTableNonTargetColumn (TableId, ColumnId)
	select c.object_id, c.column_id
	from sys.columns c
	where
		c.object_id = @TargetObjectId and
		(
			c.is_computed = 1 or
			c.user_type_id in (
				type_id('timestamp')
			)
		);

	--select *, col_name(TableId, ColumnId) from #TargetTableNonTargetColumn;
	*/

	/*
	Ensure the source table contains a column with the name contained in variable @IsDeletedColumnName.
	*/
	begin try
		if not exists (
			select 1
			from tempdb.sys.columns c
			where
				object_id = @SourceObjectId and
				name = @IsDeletedColumnName and
				is_nullable = 0 and
				user_type_id = (
					select user_type_id
					from tempdb.sys.types
					where name = 'bit'
				)
		)
		begin
			set @Msg = concat(
				'The source table must contain the following column definition: ',
				'"', @IsDeletedColumnName, ' bit not null".'
			);
			throw 50000, @Msg, 1;
		end
	end try
	begin catch
		if @@trancount != 0 rollback;
		throw;
	end catch


	/*
	Ensure that the source table and the target table have all of the same columns.
	Computed columns are ignored, as are rowversion columns.
	*/
	if (
		exists (
			select name, user_type_id, is_nullable
			from sys.columns
			where object_id = @TargetObjectId
			except
			select name, user_type_id, is_nullable
			from tempdb.sys.columns
			where object_id = @SourceObjectId and name != @IsDeletedColumnName
		) or
		exists (
			select name, user_type_id, is_nullable
			from tempdb.sys.columns
			where object_id = @SourceObjectId and name != @IsDeletedColumnName
			except
			select name, user_type_id, is_nullable
			from sys.columns
			where object_id = @TargetObjectId
		)
	)
	begin
		;throw 50000, N'The source table must have all of the same columns as the target table.', 1;
	end

	/*
	Ensure that the source table and the target table have the same primary key.
	*/
	if (
		exists (
			select c.name, c.user_type_id, c.column_id
			from sys.indexes i
			join sys.index_columns ic on i.object_id = ic.object_id
			join sys.columns c on i.object_id = c.object_id and ic.column_id = c.column_id
			where i.object_id = @TargetObjectId and i.is_primary_key = 1
			except
			select c.name, c.user_type_id, c.column_id
			from tempdb.sys.indexes i
			join tempdb.sys.index_columns ic on i.object_id = ic.object_id
			join tempdb.sys.columns c on i.object_id = c.object_id and ic.column_id = c.column_id
			where i.object_id = @SourceObjectId and i.is_primary_key = 1
		) or
		exists (
			select c.name, c.user_type_id, c.column_id
			from tempdb.sys.indexes i
			join tempdb.sys.index_columns ic on i.object_id = ic.object_id
			join tempdb.sys.columns c on i.object_id = c.object_id and ic.column_id = c.column_id
			where i.object_id = @SourceObjectId and i.is_primary_key = 1
			except
			select c.name, c.user_type_id, c.column_id
			from sys.indexes i
			join sys.index_columns ic on i.object_id = ic.object_id
			join sys.columns c on i.object_id = c.object_id and ic.column_id = c.column_id
			where i.object_id = @TargetObjectId and i.is_primary_key = 1
		)
	)
	begin
		;throw 50000, N'The source table and the target table must have the same primary key columns.', 1;
	end

	--select * from tempdb.sys.columns where object_id = @SourceObjectId;
	--select * from tempdb.sys.columns where object_id = object_id('tempdb..#sp_Merge_DeletedColumnTemplate');
	--select * from sys.columns where object_id = @TargetObjectId;

	--declare @Cr nchar(1) = nchar(13);
	--declare @Lf nchar(1) = nchar(10);
	--declare @CrLf nchar(2) = concat(@Cr, @Lf);
	--declare @Tab nchar(1) = nchar(9);

	declare @ColumnNamesCommaSeparated nvarchar(max) = N'';
	declare @ColumnDeclarationsCommaSeparated nvarchar(max) = N'';
	declare @DeletedOutputColumnNames nvarchar(max) = N'';

	select
		@ColumnDeclarationsCommaSeparated += concat(
			quotename(c.name),
			' ',
			quotename(type_name(c.user_type_id)),
			case when c.is_nullable = 0 then ' not null' else '' end,
			', '
		),
		@ColumnNamesCommaSeparated += concat(quotename(c.name), ', '),
		@DeletedOutputColumnNames += concat('deleted.', quotename(c.name), ', ')
	from sys.columns c
	where
		c.object_id = @TargetObjectId and
		c.is_computed = 0 and
		c.user_type_id not in (type_id('timestamp'))
	order by
		c.object_id,
		c.column_id;

	set @ColumnNamesCommaSeparated = substring(@ColumnNamesCommaSeparated, 1, len(@ColumnNamesCommaSeparated) - 1);
	set @ColumnDeclarationsCommaSeparated = substring(@ColumnDeclarationsCommaSeparated, 1, len(@ColumnDeclarationsCommaSeparated) - 1);
	set @DeletedOutputColumnNames = substring(@DeletedOutputColumnNames, 1, len(@DeletedOutputColumnNames) - 1);

	--print @Query;

	--select @ColumnNamesCommaSeparated, @ColumnDeclarationsCommaSeparated;

	declare @KeysEqualWhereClausePredicate nvarchar(max) = N'';
	declare @PrimaryKeyColumnList nvarchar(max) = N'';

	declare @HasUpdateableColumns bit = 0;
	declare @UpdateColumnList nvarchar(max) = N'';
	declare @UpdateColumnWhereClausePredicate nvarchar(max) = N'';
	declare @UpdateTableJoinPredicate nvarchar(max) = N'';

	select
		@KeysEqualWhereClausePredicate += concat(
			case when len(@KeysEqualWhereClausePredicate) > 0 then ' and ' else '' end,
			's.', c.name, ' = ', 't.', c.name
		),
		@PrimaryKeyColumnList += concat(
			case when len(@PrimaryKeyColumnList) > 0 then ', ' else '' end,
			c.name
		)
	from sys.columns c
	join sys.index_columns ic on c.object_id = ic.object_id and c.column_id = ic.column_id
	join sys.indexes i on c.object_id = i.object_id
	where
		c.object_id = @TargetObjectId and
		i.is_primary_key = 1
	order by
		c.object_id,
		ic.index_column_id;

	select
		@HasUpdateableColumns = 1,
		@UpdateColumnList += concat(
			case when len(@UpdateColumnList) > 0 then ', ' else '' end,
			't.', c.name, ' = ', 's.', c.name
		),
		@UpdateColumnWhereClausePredicate += concat(
			case when len(@UpdateColumnWhereClausePredicate) > 0 then ' or ' else '' end,
			case
				when c.is_nullable = 1
				then replace(N'
						(
							t.[##COL_NAME##] is null and s.[##COL_NAME##] is not null or
							t.[##COL_NAME##] is not null and s.[##COL_NAME##] is null or
							t.[##COL_NAME##] is not null and s.[##COL_NAME##] is not null and t.[##COL_NAME##] != s.[##COL_NAME##]
						)',
				'##COL_NAME##',
				c.name
				)
				else replace(N't.[##COL_NAME##] != s.[##COL_NAME##]', '##COL_NAME##', c.name)
			end
		)
	from sys.columns c
	where
		c.object_id = @TargetObjectId and
		c.is_computed = 0 and
		c.user_type_id != type_id('timestamp') and
		not exists (
			select 1
			from sys.index_columns ic
			join sys.indexes i on i.object_id = ic.object_id
			where
				i.object_id = c.object_id and
				ic.column_id = c.column_id and
				i.is_primary_key = 1
		);

	--select @UpdateColumnList, @UpdateColumnWhereClausePredicate;
	--select @KeysEqualWhereClausePredicate, @PrimaryKeyColumnList;

	set @Query = N'
	set nocount, xact_abort on;

	set @Done = 0;

	declare @Batch table (
		##COL_DECLARATIONS_COMMA_SEPARATED##,
		primary key (##PK_COLUMN_LIST##)
	);

	delete top (@BatchSize) s
	output ##DELETED_COLUMN_LIST##
	into @Batch
	from [##SOURCE_TABLE##] s
	where
		[##IS_DELETED_COLUMN_NAME##] = 0;

	if not exists (select 1 from @Batch)
	begin
		raiserror(N''Done.'', 0, 1) with nowait;
		set @Done = 1;
		return;
	end

	if exists (
		select 1
		from @Batch s
		where not exists (
			select 1
			from [##TARGET_TABLE##] t
			where
				##KEYS_EQUAL_WHERE_CLAUSE_PREDICATE##
		)
	)
	begin
		begin try
			begin transaction;

			if exists (
				select 1
				from @Batch s
				where not exists (
					select 1
					from [##TARGET_TABLE##] t with (rowlock, serializable, updlock)
					where
						##KEYS_EQUAL_WHERE_CLAUSE_PREDICATE##
				)
			)
			begin
				insert into [##TARGET_TABLE##] (##COL_NAMES_COMMA_SEPARATED##)
				select ##COL_NAMES_COMMA_SEPARATED##
				from @Batch s
				where not exists (
					select 1
					from [##TARGET_TABLE##] t
					where
						##KEYS_EQUAL_WHERE_CLAUSE_PREDICATE##
				);

				raiserror(N''Inserted batch.'', 0, 1) with nowait;
			end

			commit;
		end try
		begin catch
			if @@trancount != 0 rollback;
			throw;
		end catch
	end
	else
	begin
		raiserror(N''No rows in this batch need insertion.'', 0, 1) with nowait;
	end

	if exists (
		select 1
		from @Batch s
		where
			exists (
				select 1
				from [##TARGET_TABLE##] t
				where
					##KEYS_EQUAL_WHERE_CLAUSE_PREDICATE## and
					(
						##UPDATE_TARGET_COLUMNS_NOT_EQUAL_PREDICATE##
					)
			)
	)
	begin
		begin try
			begin transaction;

			if exists (
				select 1
				from @Batch s
				where
					exists (
						select 1
						from [##TARGET_TABLE##] t with (rowlock, serializable, updlock)
						where
							##KEYS_EQUAL_WHERE_CLAUSE_PREDICATE## and
							(
								##UPDATE_TARGET_COLUMNS_NOT_EQUAL_PREDICATE##
							)
					)
			)
			begin
				update t
				set ##UPDATE_COLUMNS##
				from [##TARGET_TABLE##] t
				join @Batch s on ##KEYS_EQUAL_WHERE_CLAUSE_PREDICATE##
				where
					##UPDATE_TARGET_COLUMNS_NOT_EQUAL_PREDICATE##;
				
				raiserror(N''Rows updated'', 0, 1) with nowait;
			end

			commit;
		end try
		begin catch
			if @@trancount != 0 rollback;
			throw;
		end catch
	end
	';

	set @Query = replace(@Query, '##COL_DECLARATIONS_COMMA_SEPARATED##', @ColumnDeclarationsCommaSeparated);
	set @Query = replace(@Query, '##DELETED_COLUMN_LIST##', @DeletedOutputColumnNames);
	set @Query = replace(@Query, '##SOURCE_TABLE##', @SourceTempTableName);
	set @Query = replace(@Query, '##PK_COLUMN_LIST##', @PrimaryKeyColumnList);
	set @Query = replace(@Query, '##TARGET_TABLE##', @TargetTableName);
	set @Query = replace(@Query, '##KEYS_EQUAL_WHERE_CLAUSE_PREDICATE##', @KeysEqualWhereClausePredicate);
	set @Query = replace(@Query, '##COL_NAMES_COMMA_SEPARATED##', @ColumnNamesCommaSeparated);
	set @Query = replace(@Query, '##IS_DELETED_COLUMN_NAME##', @IsDeletedColumnName);
	set @Query = replace(@Query, '##UPDATE_TARGET_COLUMNS_NOT_EQUAL_PREDICATE##', @UpdateColumnWhereClausePredicate);
	set @Query = replace(@Query, '##UPDATE_WHERE_CLAUSE##', '1=1');
	set @Query = replace(@Query, '##UPDATE_COLUMNS##', @UpdateColumnList);

	--print @Query;
	--return;

	declare @Done bit = 0;

	while @Done = 0
	begin
		exec sp_executesql
			@stmt = @Query,
			@params = N'@BatchSize int, @Done bit output',
			@BatchSize = @BatchSize,
			@Done = @Done output;
	end


	set @Query = N'
	set nocount, xact_abort on;

	set @Done = 0;

	declare @Batch table (
		##COL_DECLARATIONS_COMMA_SEPARATED##,
		primary key (##PK_COLUMN_LIST##)
	);

	delete top (@BatchSize) s
	output ##DELETED_COLUMN_LIST##
	into @Batch
	from [##SOURCE_TABLE##] s
	where
		[##IS_DELETED_COLUMN_NAME##] = 1;

	if not exists (select 1 from @Batch)
	begin
		raiserror(N''Done.'', 0, 1) with nowait;
		set @Done = 1;
		return;
	end

	if exists (
		select 1
		from @Batch s
		where exists (
			select 1
			from [##TARGET_TABLE##] t
			where
				##KEYS_EQUAL_WHERE_CLAUSE_PREDICATE##
		)
	)
	begin
		begin try
			delete t
			from [##TARGET_TABLE##] t
			where exists (
				select 1
				from @Batch s
				where
					##KEYS_EQUAL_WHERE_CLAUSE_PREDICATE##
			);

			raiserror(N''Deleted batch.'', 0, 1) with nowait;
		end try
		begin catch
			if @@trancount != 0 rollback;
			throw;
		end catch
	end
	else
	begin
		raiserror(N''No rows in this batch exist.'', 0, 1) with nowait;
	end
	';

	set @Query = replace(@Query, '##COL_DECLARATIONS_COMMA_SEPARATED##', @ColumnDeclarationsCommaSeparated);
	set @Query = replace(@Query, '##DELETED_COLUMN_LIST##', @DeletedOutputColumnNames);
	set @Query = replace(@Query, '##SOURCE_TABLE##', @SourceTempTableName);
	set @Query = replace(@Query, '##PK_COLUMN_LIST##', @PrimaryKeyColumnList);
	set @Query = replace(@Query, '##TARGET_TABLE##', @TargetTableName);
	set @Query = replace(@Query, '##KEYS_EQUAL_WHERE_CLAUSE_PREDICATE##', @KeysEqualWhereClausePredicate);
	set @Query = replace(@Query, '##COL_NAMES_COMMA_SEPARATED##', @ColumnNamesCommaSeparated);
	set @Query = replace(@Query, '##IS_DELETED_COLUMN_NAME##', @IsDeletedColumnName);

	set @Done = 0;

	while @Done = 0
	begin
		exec sp_executesql
			@stmt = @Query,
			@params = N'@BatchSize int, @Done bit output',
			@BatchSize = @BatchSize,
			@Done = @Done output;
	end

	return 0;
end

go

set nocount on;
go

drop table if exists #Temp, dbo.MyTable;
go
create table #Temp (X int not null default 1, Id int, C1 int not null default 0, C2 int, C3 int, IsRowDeleted bit not null default 0, RV rowversion, MyComputedColumn as 1, primary key (Id, C1));
go

insert into #Temp (Id) values (1), (2), (3), (4), (5), (20);
go
update #Temp set IsRowDeleted = 1 where Id in (2, 20);
update #Temp set C3 = 1;
go

create table dbo.MyTable (X int not null default 1, Id int, C1 int not null default 0, C2 int default 0, C3 int, RV rowversion, MyComputedColumn as 1, primary key (Id, C1));
go
insert into dbo.MyTable (Id) values (1), (2);
go

--select * from tempdb.sys.tables;
select 'Before', * from #Temp;
select 'Before', * from dbo.MyTable;
go

exec sp_Merge
	@SourceTempTableName = N'#Temp',
	@TargetSchemaName = N'dbo',
	@TargetTableName = N'MyTable',
	@BatchSize = 1,
	@IsDeletedColumnName = N'IsRowDeleted';
go

select 'After', * from #Temp;
select 'After', * from dbo.MyTable;
go
