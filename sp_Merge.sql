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
	@StagingTempTableName sysname,
	@TargetSchemaName sysname,
	@TargetTableName sysname,
	@BatchSize int = 1000,
	@Debug bit = 0
as
begin
	if @@trancount != 0 throw 50000, N'Open transaction not allowed.', 1;

	declare @SourceTempDbObjectId int = object_id(concat('tempdb..', @StagingTempTableName), 'U');
	if @SourceTempDbObjectId is null throw 50000, N'Staging table must exist.', 1;
	if @SourceTempDbObjectId > 0 throw 50000, N'Staging table must be a temporary table (e.g.: #Temp), not a global temporary table (e.g.: ##Temp).', 1;
	if not exists (select 1 from tempdb.sys.indexes where object_id = @SourceTempDbObjectId and is_primary_key = 1) throw 50000, N'Staging table must have a primary key.', 1;

	declare @TargetName nvarchar(261) = concat(quotename(@TargetSchemaName), '.', quotename(@TargetTableName));
	declare @TargetObjectId int = object_id(@TargetName, 'U');
	if @TargetObjectId is null throw 50000, N'@TargetSchemaName and @TargetTableName must refer to a table that exists.', 1;
	if not exists (select 1 from sys.indexes where object_id = @TargetObjectId and is_primary_key = 1) throw 50000, N'Target table must have a primary key.', 1;

	--select
	--	@StagingTempTableName,
	--	@SourceTempDbObjectId;

	declare @Query nvarchar(max);

	/*
	Ensure the staging table contains the sp_Merge__RowIsDeleted column.
	*/
	begin try
		create table #sp_Merge_DeletedColumnTemplate (
			sp_Merge__RowIsDeleted bit not null
		);

		if exists (
			select name, user_type_id, is_nullable
			from tempdb.sys.columns
			where object_id = object_id('tempdb..#sp_Merge_DeletedColumnTemplate')
			except
			select name, user_type_id, is_nullable
			from tempdb.sys.columns
			where object_id = @SourceTempDbObjectId
		)
		begin
			;throw 50000, N'The staging table must contain the following column definition: "sp_Merge__RowIsDeleted bit not null".', 1;
		end
	end try
	begin catch
		if @@trancount != 0 rollback;
		throw;
	end catch


	/*
	Ensure that the staging table and the target table have all of the same columns.
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
			where object_id = @SourceTempDbObjectId and name != 'sp_Merge__RowIsDeleted'
		) or
		exists (
			select name, user_type_id, is_nullable
			from tempdb.sys.columns
			where object_id = @SourceTempDbObjectId and name != 'sp_Merge__RowIsDeleted'
			except
			select name, user_type_id, is_nullable
			from sys.columns
			where object_id = @TargetObjectId
		)
	)
	begin
		;throw 50000, N'The staging table must have all of the same columns as the target table.', 1;
	end

	/*
	Ensure that the staging table and the target table have the same primary key.
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
			where i.object_id = @SourceTempDbObjectId and i.is_primary_key = 1
		) or
		exists (
			select c.name, c.user_type_id, c.column_id
			from tempdb.sys.indexes i
			join tempdb.sys.index_columns ic on i.object_id = ic.object_id
			join tempdb.sys.columns c on i.object_id = c.object_id and ic.column_id = c.column_id
			where i.object_id = @SourceTempDbObjectId and i.is_primary_key = 1
			except
			select c.name, c.user_type_id, c.column_id
			from sys.indexes i
			join sys.index_columns ic on i.object_id = ic.object_id
			join sys.columns c on i.object_id = c.object_id and ic.column_id = c.column_id
			where i.object_id = @TargetObjectId and i.is_primary_key = 1
		)
	)
	begin
		;throw 50000, N'The staging table and the target table must have the same primary key columns.', 1;
	end

	--select * from tempdb.sys.columns where object_id = @SourceTempDbObjectId;
	--select * from tempdb.sys.columns where object_id = object_id('tempdb..#sp_Merge_DeletedColumnTemplate');
	--select * from sys.columns where object_id = @TargetObjectId;

	--declare @Cr nchar(1) = nchar(13);
	--declare @Lf nchar(1) = nchar(10);
	--declare @CrLf nchar(2) = concat(@Cr, @Lf);
	--declare @Tab nchar(1) = nchar(9);

	declare @ColumnNamesCommaSeparated nvarchar(max) = N'';
	declare @ColumnDeclarationsCommaSeparated nvarchar(max) = N'';
	declare @DeletedOutputColummNames nvarchar(max) = N'';

	select
		@ColumnDeclarationsCommaSeparated += concat(
			quotename(c.name),
			' ',
			quotename(type_name(c.user_type_id)),
			case when c.is_nullable = 0 then ' not null' else '' end,
			', '
		),
		@ColumnNamesCommaSeparated += concat(quotename(c.name), ', '),
		@DeletedOutputColummNames += concat('deleted.', quotename(c.name), ', ')
	from sys.columns c
	where
		c.object_id = @TargetObjectId and
		c.is_computed = 0 and
		c.user_type_id not in (type_id('timestamp'))
	order by
		c.column_id;

	set @ColumnNamesCommaSeparated = substring(@ColumnNamesCommaSeparated, 1, len(@ColumnNamesCommaSeparated) - 1);
	set @ColumnDeclarationsCommaSeparated = substring(@ColumnDeclarationsCommaSeparated, 1, len(@ColumnDeclarationsCommaSeparated) - 1);
	set @DeletedOutputColummNames = substring(@DeletedOutputColummNames, 1, len(@DeletedOutputColummNames) - 1);

	--print @Query;

	--select @ColumnNamesCommaSeparated, @ColumnDeclarationsCommaSeparated;

	declare @KeysEqualWhereClausePredicate nvarchar(max) = N'';
	declare @PrimaryKeyColumnList nvarchar(max) = N'';

	select
		@KeysEqualWhereClausePredicate += concat('s.', c.name, ' = ', 't.', c.name),
		@PrimaryKeyColumnList += c.name
	from sys.columns c
	join sys.index_columns ic on c.object_id = ic.object_id and c.column_id = ic.column_id
	join sys.indexes i on c.object_id = i.object_id
	where
		c.object_id = @TargetObjectId and
		i.is_primary_key = 1 and
		ic.index_column_id = 1
	order by
		c.object_id,
		ic.index_column_id;

	select
		@KeysEqualWhereClausePredicate += concat(' and ', 's.', c.name, ' = ', 't.', c.name),
		@PrimaryKeyColumnList += concat(', ', c.name)
	from sys.columns c
	join sys.index_columns ic on c.object_id = ic.object_id and c.column_id = ic.column_id
	join sys.indexes i on c.object_id = i.object_id
	where
		c.object_id = @TargetObjectId and
		i.is_primary_key = 1 and
		ic.index_column_id > 1
	order by
		c.object_id,
		ic.index_column_id;

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
		sp_Merge__RowIsDeleted = 0;

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
	';

	set @Query = replace(@Query, '##COL_DECLARATIONS_COMMA_SEPARATED##', @ColumnDeclarationsCommaSeparated);
	set @Query = replace(@Query, '##DELETED_COLUMN_LIST##', @DeletedOutputColummNames);
	set @Query = replace(@Query, '##SOURCE_TABLE##', @StagingTempTableName);
	set @Query = replace(@Query, '##PK_COLUMN_LIST##', @PrimaryKeyColumnList);
	set @Query = replace(@Query, '##TARGET_TABLE##', @TargetTableName);
	set @Query = replace(@Query, '##KEYS_EQUAL_WHERE_CLAUSE_PREDICATE##', @KeysEqualWhereClausePredicate);
	set @Query = replace(@Query, '##COL_NAMES_COMMA_SEPARATED##', @ColumnNamesCommaSeparated);

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
		sp_Merge__RowIsDeleted = 1;

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
			begin transaction;

			delete t
			from [##TARGET_TABLE##] t
			where exists (
				select 1
				from @Batch s
				where
					##KEYS_EQUAL_WHERE_CLAUSE_PREDICATE##
			);

			raiserror(N''Deleted batch.'', 0, 1) with nowait;

			commit;
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
	set @Query = replace(@Query, '##DELETED_COLUMN_LIST##', @DeletedOutputColummNames);
	set @Query = replace(@Query, '##SOURCE_TABLE##', @StagingTempTableName);
	set @Query = replace(@Query, '##PK_COLUMN_LIST##', @PrimaryKeyColumnList);
	set @Query = replace(@Query, '##TARGET_TABLE##', @TargetTableName);
	set @Query = replace(@Query, '##KEYS_EQUAL_WHERE_CLAUSE_PREDICATE##', @KeysEqualWhereClausePredicate);
	set @Query = replace(@Query, '##COL_NAMES_COMMA_SEPARATED##', @ColumnNamesCommaSeparated);

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
create table #Temp (X int, Id int, C1 int not null default 0, sp_Merge__RowIsDeleted bit not null default 0, RV rowversion, MyComputedColumn as 1, primary key (Id, C1));
go

insert into #Temp (Id) values (1), (2), (3), (4), (5), (20);
go
update #Temp set sp_Merge__RowIsDeleted = 1 where Id in (2, 20);
go

create table dbo.MyTable (X int, Id int, C1 int not null default 0, RV rowversion, MyComputedColumn as 1, primary key (Id, C1));
go
insert into dbo.MyTable (Id) values (1), (2);
go

--select * from tempdb.sys.tables;
select 'Before', * from #Temp;
select 'Before', * from dbo.MyTable;
go

exec sp_Merge '#Temp', N'dbo', N'MyTable', @BatchSize = 1;
go

select 'After', * from #Temp;
select 'After', * from dbo.MyTable;
go

--select c.name, ic.index_column_id
--from sys.key_constraints kc
--join sys.indexes i on kc.parent_object_id = i.object_id and kc.unique_index_id = i.index_id
--join sys.index_columns ic on i.object_id = ic.object_id
--join sys.columns c on ic.object_id = c.object_id and ic.column_id = c.column_id
--where kc.type = 'PK' and kc.parent_object_id = object_id('MyTable') and is_included_column = 0;

--select *
--from sys.indexes i
--join sys.index_columns ic on i.object_id = ic.object_id
--join sys.columns c on ic.object_id = c.object_id and ic.column_id = c.column_id
--where
--	i.object_id = object_id('MyTable')
--order by
--	i.object_id,
--	ic.index_column_id;

--select * from sys.columns
