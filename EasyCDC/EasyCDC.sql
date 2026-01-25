use master;
go

set ansi_nulls on;
set ansi_padding on;
set ansi_warnings on;
set arithabort on;
set concat_null_yields_null on;
set quoted_identifier on;
set statistics io off;
set statistics time off;
go

set nocount, xact_abort on;
go

if db_id('EasyCDC') is null
begin
	exec('create database EasyCDC;');
end
go

use EasyCDC;
go

if not exists (
	select 1
	from sys.databases
	where
		database_id = db_id() and
		compatibility_level = 100
)
begin
	exec('alter database EasyCDC set compatibility_level = 100;');
end
go

--select * from sys.databases;
go

drop procedure if exists
	EasyCDC.ConfigureCDC,
	EasyCDC.ApplyDesiredState,
	EasyCDC.MergeConfig,
	EasyCDC.UpdateConsumerProgress;
go

drop table if exists
	EasyCDC.SourceColumn,
	EasyCDC.SourceTable,
	EasyCDC.Config,
	EasyCDC.ConsumedCaptureInstance,
	EasyCDC.Consumer;
go

drop trigger if exists DatabaseTrigger on database;
go

drop type if exists	EasyCDC.SourceTable;
drop type if exists EasyCDC.SourceColumn;
drop type if exists EasyCDC.Consumer;
drop type if exists EasyCDC.ConsumedCaptureInstance
go

drop schema if exists EasyCDC;
go

drop user if exists EasyCDCExecUser;
go

create schema EasyCDC;
go

create table EasyCDC.Config (
	RowNumber int not null
		constraint EasyCDC_Config_RowNumber_DF default 1
		constraint EasyCDC_Config_RowNumber_CK check (RowNumber = 1)
		constraint UC_EasyCDC_Config_PK primary key (RowNumber),
	BlockAddColumn bit not null
		constraint EasyCDC_Config_BlockAddColumn_DF default 1,
	BlockDropColumn bit not null
		constraint EasyCDC_Config_BlockDropColumn_DF default 1,
	DisableCdcOnUnconfiguredTables bit not null
		constraint EasyCDC_Config_DisableCdcOnUnconfiguredTables_DF default 1,
	SuffixKind nvarchar(20) not null
		constraint EasyCDC_Config_SuffixKind_DF default 'ISO8601'
		constraint EasyCDC_Config_SuffixKind_CK
		check (SuffixKind in ('UnixEpoch', 'ISO8601'))
);
go

create table EasyCDC.Consumer (
	ConsumerName sysname not null
		constraint UC_EasyCDC_Consumer_PK
		primary key clustered (ConsumerName)
);
go

create table EasyCDC.ConsumedCaptureInstance (
	ConsumerName sysname not null,
	CaptureInstanceName sysname not null,

	constraint UC_EasyCDC_ConsumedCaptureInstance_PK
	primary key clustered (ConsumerName, CaptureInstanceName),

	constraint EasyCDC_Consumer_discloses_ConsumedCaptureInstance_FK
	foreign key (ConsumerName)
	references EasyCDC.Consumer (ConsumerName)
);
go

insert into EasyCDC.Config default values;
go

--select * from EasyCDC.Config;
go

create table EasyCDC.SourceTable (
	SchemaName sysname not null,
	TableName sysname not null,
	CaptureInstancePrefix nvarchar(80) not null,
	SupportsNetChanges bit not null,
	RoleName sysname null,
	IndexName sysname null,
	FilegroupName sysname null,
	AllowPartitionSwitch bit not null,

	constraint UC_EasyCDC_SourceTable_PK
	primary key clustered (SchemaName, TableName)
);
go

create table EasyCDC.SourceColumn (
	SchemaName sysname not null,
	TableName sysname not null,
	ColumnName sysname not null,

	constraint UC_EasyCDC_SourceColumn_PK
	primary key clustered (SchemaName, TableName, ColumnName),

	constraint EasyCDC_SourceTable_comprises_SourceColumn_FK
	foreign key (SchemaName, TableName)
	references EasyCDC.SourceTable (SchemaName, TableName)
);
go

create user EasyCDCExecUser without login;
go

alter role db_owner add member EasyCDCExecUser;
go

create trigger DatabaseTrigger
on database
with execute as 'EasyCDCExecUser'
for alter_table
as
begin
	if not exists (
		select 1
		from EasyCDC.Config
		where
			BlockAddColumn = 1 or
			BlockDropColumn = 1
	)
	begin
		return;
	end
end
go

create procedure EasyCDC.ApplyDesiredState
	@Mode nvarchar(20) = 'Commit'
with execute as 'EasyCDCExecUser'
as
begin
	set nocount, xact_abort on;

	if @@trancount != 0 throw 20240520, N'Open transaction not allowed.', 1;
	if @Mode not in ('Commit', 'OutputXML', 'OutputString') throw 20240520, N'@Mode must be one of: Commit, OutputXML, OutputString.', 1;

	declare
		@HasConfigRow bit = 0,
		@BlockAddColumn bit = 0,
		@BlockDropColumn bit = 0,
		@DisableCdcOnUnconfiguredTables bit = 0,
		@SuffixKind nvarchar(20) = N'';

	select
		@HasConfigRow = 1,
		@BlockAddColumn = a.BlockAddColumn,
		@BlockDropColumn = a.BlockDropColumn,
		@DisableCdcOnUnconfiguredTables = a.DisableCdcOnUnconfiguredTables,
		@SuffixKind = a.SuffixKind
	from EasyCDC.Config a;

	if @HasConfigRow = 0 throw 20240520, N'No configuration row in table [EasyCDC].[Config].', 1;

	declare @Query nvarchar(max);

	declare @UnixEpoch bigint = datediff_big(second, '19700101', getutcdate());

	declare @ISOTimestamp nvarchar(20) = (
		select ISOTimestamp
		from (select convert(datetimeoffset(0), getutcdate())) v1(V1)
		outer apply (select convert(nvarchar(50), V1, 127)) v2(V2)
		outer apply (select replace(V2, '-', '')) v3(V3)
		outer apply (select replace(V3, ':', '')) v4(V4)
		outer apply (select convert(nvarchar(20), V4)) v5(ISOTimestamp)
	);

	declare @Suffix nvarchar(20) = iif(
		@SuffixKind = 'UnixEpoch',
		convert(nvarchar(20), @UnixEpoch),
		@ISOTimestamp
	);

	declare @CursorQuery nvarchar(max) = N'
set nocount, xact_abort on;

begin try
	declare @Query nvarchar(max);

	declare C1 cursor fast_forward local for
		select QueryOrText
		from #Query
		where IsExecutable = 1
		order by Id;

	open C1;
	fetch next from C1 into @Query;

	while @@fetch_status = 0
	begin
		exec(@Query);
		fetch next from C1 into @Query;
	end
end try
begin catch
	if @@trancount != 0 rollback;
	throw;
end catch
';

	begin try

		--select d1.*, d2.*
		--from sys.tables t
		--outer apply (select t.* for json path, include_null_values, without_array_wrapper) d1(JsonVal)
		--outer apply (select * from openjson(JsonVal) with (name sysname '$.name')) d2(TableName);

		--select 1, * from sys.tables;

		create table #Table (
			SchemaName sysname not null,
			TableName sysname not null,
			IsTracked bit not null,
			IsConfigured bit not null default 0,
			ConfiguredColumnList nvarchar(max) not null default '',

			primary key clustered (SchemaName, TableName)
		);

		create table #Column (
			SchemaName sysname not null,
			TableName sysname not null,
			ColumnName sysname not null,
			IsConfigured bit not null default 0,

			primary key clustered (SchemaName, TableName, ColumnName)
		);

		create table #CaptureInstance (
			SourceSchemaName sysname not null,
			SourceTableName sysname not null,
			CaptureInstanceName sysname not null,
			StartLSN binary(10) null,
			SupportsNetChanges bit not null,
			RoleName sysname null,
			IndexName sysname null,
			FilegroupName sysname null,
			CreatedDate datetime not null,
			PartitionSwitch bit not null,

			primary key clustered (SourceSchemaName, SourceTableName, CaptureInstanceName)
		);

		create table #Query (
			Id int not null identity(1,1),
			QueryOrText nvarchar(max) not null,
			IsExecutable bit not null,
			IsPrintable bit not null
		);

		create table #Error (
			Id int not null identity(1,1),
			ErrorNumber int not null,
			ErrorMessage nvarchar(max) not null,
			ErrorLine int not null,
			ErrorSeverity int not null,
			ErrorState int not null
		);

		--select * from cdc.change_tables;
		--exec sp_help 'cdc.change_tables';

		--create table #DroppableCaptureInstance (
		--	TableId int not null primary key,
		--	QueryOrText nvarchar(max) not null default '',
		--	IsExecutable bit not null default 0
		--);

		insert into #Table (SchemaName, TableName, IsTracked)
		select
			object_schema_name(t.object_id),
			t.name,
			t.is_tracked_by_cdc
		from sys.tables t
		where
			t.is_ms_shipped = 0;

		insert into #Column (SchemaName, TableName, ColumnName)
		select
			object_schema_name(t.object_id),
			t.name,
			c.name
		from sys.tables t
		join sys.columns c on t.object_id = c.object_id
		where
			t.is_ms_shipped = 0;

		insert into #CaptureInstance (
			SourceSchemaName,
			SourceTableName,
			CaptureInstanceName,
			StartLSN,
			SupportsNetChanges,
			RoleName,
			IndexName,
			FilegroupName,
			CreatedDate,
			PartitionSwitch
		)
		select
			object_schema_name(ct.source_object_id),
			object_name(ct.source_object_id),
			ct.capture_instance,
			ct.start_lsn,
			ct.supports_net_changes,
			ct.role_name,
			ct.index_name,
			ct.filegroup_name,
			ct.create_date,
			ct.partition_switch
		from cdc.change_tables ct;

		update t
		set
			t.IsConfigured = 1
		from #Table t
		where
			exists (
				select 1
				from EasyCDC.SourceTable st
				where
					t.SchemaName = st.SchemaName and
					t.TableName = st.TableName
			);

		update c
		set
			c.IsConfigured = 1
		from #Column c
		where
			exists (
				select 1
				from EasyCDC.SourceColumn sc
				where
					c.SchemaName = sc.SchemaName and
					c.TableName = sc.TableName and
					c.ColumnName = sc.ColumnName
			);

		update t
		set
			t.ConfiguredColumnList = isnull(d1.ColumnName, '')
		from #Table t
		outer apply (
			select V6
			from (
				select quotename(c.ColumnName) as ColumnName
				from #Column c
				where
					t.SchemaName = c.SchemaName and
					t.TableName = c.TableName and
					c.IsConfigured = 1
				order by
					c.SchemaName,
					c.TableName,
					c.ColumnName
				for xml path('')
			) s(V1)
			outer apply (select isnull(V1, '')) v2(V2)
			outer apply (select replace(V2, '</ColumnName><ColumnName>', ', ')) v3(V3)
			outer apply (select replace(V3, '<ColumnName>', '')) v4(V4)
			outer apply (select replace(V4, '</ColumnName>', '')) v5(V5)
			outer apply (select convert(nvarchar(max), V5)) v6(V6)
		) d1(ColumnName);

		--select * from #Table;
		--select * from #Column;
		select * from #CaptureInstance;

		--with
		--	SourceTable(TableId) as (
		--		select d2.TableId
		--		from EasyCDC.SourceTable a
		--		outer apply (select concat(quotename(a.SchemaName), '.', quotename(a.TableName))) d1(FullName)
		--		outer apply (select object_id(FullName, 'U')) d2(TableId)
		--		where
		--			d2.TableId is not null
		--	)
		--insert into #DroppableCaptureInstance (TableId)
		--select
		--	t.object_id as TableId
		--from sys.tables t
		--where
		--	t.is_tracked_by_cdc = 1 and
		--	t.object_id not in (
		--		select TableId
		--		from SourceTable a
		--	);

		insert into #Query (QueryOrText, IsExecutable, IsPrintable)
		select 'set nocount, xact_abort on;', 0, 1;

		insert into #Query (QueryOrText, IsExecutable, IsPrintable)
		select 'drop table if exists #Error;', 0, 1;

		insert into #Query (QueryOrText, IsExecutable, IsPrintable)
		select
			N'
create table #Error (
	Id int not null identity(1,1),
	ErrorNumber int not null,
	ErrorMessage nvarchar(max) not null,
	ErrorLine int not null,
	ErrorSeverity int not null,
	ErrorState int not null
);
', 0, 1;

		delete t
		output v3.V3, q.IsExecutable, q.IsPrintable
		into #Query
		from #Table t
		outer apply (
			select '
begin try
	if object_id(''[##SCHEMA_NAME##].[##TABLE_NAME##]'', ''U'') is not null
	begin
		--declare @A int = (select 1/0);

		exec sys.sp_cdc_disable_table
			@source_schema = N''##SCHEMA_NAME##'',
			@source_name = N''##TABLE_NAME##'',
			@capture_instance = N''all'';
	end
end try
begin catch
	if @@trancount != 0 rollback;

	insert into #Error (
		ErrorNumber,
		ErrorMessage,
		ErrorLine,
		ErrorSeverity,
		ErrorState
	)
	values (
		error_number(),
		error_message(),
		error_line(),
		error_severity(),
		error_state()
	);
end catch
'
		) v1(V1)
		outer apply (select replace(V1, '##SCHEMA_NAME##', t.SchemaName)) v2(V2)
		outer apply (select replace(V2, '##TABLE_NAME##', t.TableName)) v3(V3)
		outer apply (select convert(bit, 1), convert(bit, 1)) q(IsExecutable, IsPrintable)
		where
			t.IsConfigured = 0 and
			t.IsTracked = 1 and
			@DisableCdcOnUnconfiguredTables = 1;

		delete t
		from #Table t
		where
			t.IsConfigured = 0 and
			t.IsTracked = 1;

		select * from #Query;
		--return;


--		update #DroppableCaptureInstance
--		set
--			IsExecutable = iif(@DisableCdcOnUnconfiguredTables = 1, 1, 0),
--			QueryOrText = iif(
--				@DisableCdcOnUnconfiguredTables = 1,
--N'
--set nocount, xact_abort on;

--begin try
--	--declare @X int = (select 1/0);

--	exec sys.sp_cdc_disable_table
--		@source_schema = N''##SCHEMA_NAME##'',
--		@source_name = N''##TABLE_NAME##'',
--		@capture_instance = N''all'';
--end try
--begin catch
--	if @@trancount != 0 rollback;
--	throw;
--end catch
--',
--N'
--/*
--CDC is enabled for table [##SCHEMA_NAME##].[##TABLE_NAME##], but it will not
--be disabled because [EasyCDC].[Config].[DisableCdcOnUnconfiguredTables] is
--set to 0 (false).
--*/
--'
--			);

		--update #DroppableCaptureInstance set QueryOrText = replace(QueryOrText, '##SCHEMA_NAME##', object_schema_name(TableId));
		--update #DroppableCaptureInstance set QueryOrText = replace(QueryOrText, '##TABLE_NAME##', object_name(TableId));

--		insert into #Query (QueryOrText, IsExecutable)
--		select
--N'
--/*
----------------------------------------------------------------------------------
--DROP UNSPECIFIED CAPTURE INSTANCES
----------------------------------------------------------------------------------
--*/
--', 0;

		--insert into #Query (QueryOrText, IsExecutable)
		--select QueryOrText, IsExecutable
		--from #DroppableCaptureInstance;

--		insert into #Query (QueryOrText, IsExecutable)
--		select
--N'
--/*
----------------------------------------------------------------------------------
--END OF DROP UNSPECIFIED CAPTURE INSTANCES
----------------------------------------------------------------------------------
--*/
--', 0;

		if @Mode = 'Commit'
		begin
			exec(@CursorQuery);

			select
				'SUMMARY' as RowKind,
				iif(exists(select 1 from #Error), 'FAIL', 'SUCCESS') as Outcome,
				null as ErrorNumber,
				null as ErrorMessage,
				null as ErrorLine,
				null as ErrorState,
				null as ErrorSeverity
			union all
			select
				'ERROR' as RowKind,
				'ERROR' as Outcome,
				ErrorNumber,
				ErrorMessage,
				ErrorLine,
				ErrorState,
				ErrorSeverity
			from #Error;
		end
		else
		begin
			declare
				@Output nvarchar(max) = N'',
				@CrLf nchar(2) = concat(nchar(13), nchar(10));

			select @Output += concat('-- Query: ', Id, @CrLf, QueryOrText, @CrLf, 'go', @CrLf, @CrLf)
			from #Query
			order by Id;

			if @Mode = 'OutputXML'
			begin
				select
					convert(
						xml,
						concat('<?GeneratedCode --', @CrLf, @Output, @CrLf, '--?>')
					) as GeneratedOutput;
			end
			else
			begin
				select @Output as GeneratedOutput;
			end
		end

		--select a.*
		--into #Temp
		--from (
		--	select
		--		object_schema_name(t.object_id) as SchemaName,
		--		t.name as TableName,
		--		t.is_tracked_by_cdc as IsTrackedByCDC
		--	from sys.tables t
		--) t
		--outer apply (
		--	select *
		--	from (select convert(varchar(20), 'TABLE')) a(RowType)
		--	outer apply (select t.* for json path, include_null_values, without_array_wrapper) d1(JsonVal)
		--) a
		--union all
		--select a.*
		--from (
		--	select
		--		object_schema_name(i.object_id) as SchemaName,
		--		object_name(i.object_id) as ObjectName,
		--		i.name as IndexName,
		--		i.is_unique as IsUnique,
		--		i.is_primary_key as IsPrimaryKey,
		--		i.is_unique_constraint as IsUniqueConstraint
		--	from sys.indexes i
		--	where
		--		exists (
		--			select 1
		--			from sys.tables t
		--			where
		--				i.object_id = t.object_id
		--		)
		--) i
		--outer apply (
		--	select *
		--	from (select convert(varchar(20), 'INDEX')) a(RowType)
		--	outer apply (select i.* for json path, include_null_values, without_array_wrapper) d1(JsonVal)
		--) a;

	end try
	begin catch
		if @@trancount != 0 rollback;
		throw;
	end catch
end
go

create procedure EasyCDC.UpdateConsumerProgress
as
begin
	set nocount, xact_abort on;

	if @@trancount != 0 throw 20240520, N'Open transaction not allowed.', 1;
end
go

create type EasyCDC.SourceTable as table (
	SchemaName sysname not null,
	TableName sysname not null,
	CaptureInstancePrefix nvarchar(80) not null,
	SupportsNetChanges bit not null default 1,
	RoleName sysname null,
	IndexName sysname null,
	FilegroupName sysname null,
	AllowPartitionSwitch bit not null default 1,

	primary key clustered (SchemaName, TableName)
);
go

create type EasyCDC.SourceColumn as table (
	SchemaName sysname not null,
	TableName sysname not null,
	ColumnName sysname not null,

	primary key clustered (SchemaName, TableName, ColumnName)
);
go

create type EasyCDC.Consumer as table (
	ConsumerName sysname not null primary key
);
go

create type EasyCDC.ConsumedCaptureInstance as table (
	ConsumerName sysname not null,
	CaptureInstanceName sysname not null,
	primary key clustered (ConsumerName, CaptureInstanceName)
);
go

create procedure EasyCDC.MergeConfig
	@SourceTable EasyCDC.SourceTable readonly,
	@SourceColumn EasyCDC.SourceColumn readonly
as
begin
	set nocount, xact_abort on;

	if @@trancount != 0 throw 20240520, N'Open transaction not allowed.', 1;

	begin try
		begin transaction;

		declare @GetLocks bit =
			iif(
				exists(select 1 from EasyCDC.SourceTable with (serializable, tablockx)) and
				exists(select 1 from EasyCDC.SourceColumn with (serializable, tablockx)),
				1,
				0
			);

		delete EasyCDC.SourceColumn;
		delete EasyCDC.SourceTable;

		insert into EasyCDC.SourceTable (
			SchemaName,
			TableName,
			CaptureInstancePrefix,
			SupportsNetChanges,
			RoleName,
			IndexName,
			FilegroupName,
			AllowPartitionSwitch
		)
		select
			SchemaName,
			TableName,
			CaptureInstancePrefix,
			SupportsNetChanges,
			RoleName,
			IndexName,
			FilegroupName,
			AllowPartitionSwitch
		from @SourceTable;

		insert into EasyCDC.SourceColumn (
			SchemaName,
			TableName,
			ColumnName
		)
		select
			SchemaName,
			TableName,
			ColumnName
		from @SourceColumn;

		commit;
	end try
	begin catch
		if @@trancount != 0 rollback;
		throw;
	end catch
end
go

create or alter procedure dbo.Setup
as
begin
	set nocount, xact_abort on;

	if not exists (
		select 1
		from sys.databases
		where
			database_id = db_id() and
			owner_sid = 0x01
	)
	begin
		exec('alter authorization on database::EasyCDC to sa;');
	end

	if not exists (
		select 1
		from sys.databases
		where
			database_id = db_id() and
			is_cdc_enabled = 1
	)
	begin
		exec sys.sp_cdc_enable_db;
	end

	if object_id('dbo.Tracked') is null
	begin
		exec('create table dbo.Tracked (Id int primary key, T1 int null, T2 int null, U1 int null, U2 int null);');
	end

	if object_id('dbo.TrackedV2') is null
	begin
		exec('create table dbo.TrackedV2 (Id int primary key, T1 int null, T2 int null, U1 int null, U2 int null);');
	end

	if object_id('dbo.Untracked') is null
	begin
		exec('create table dbo.Untracked (Id int primary key, C1 int null);');
	end

	if object_id('dbo.UntrackedV2') is null
	begin
		exec('create table dbo.UntrackedV2 (Id int primary key, C1 int null);');
	end

	if object_id('dbo.ShouldBeTracked') is null
	begin
		exec('create table dbo.ShouldBeTracked (Id int primary key, T1 int null, T2 int null, U1 int null, U2 int null);');
	end

	if not exists (
		select 1
		from sys.tables t
		where
			t.object_id = object_id('dbo.Tracked') and
			t.is_tracked_by_cdc = 1
	)
	begin
		exec sys.sp_cdc_enable_table 'dbo', 'Tracked', 'dbo_Tracked', @role_name = null;
	end

	if not exists (
		select 1
		from sys.tables t
		where
			t.object_id = object_id('dbo.TrackedV2') and
			t.is_tracked_by_cdc = 1
	)
	begin
		exec sys.sp_cdc_enable_table 'dbo', 'TrackedV2', 'dbo_TrackedV2', @role_name = null;
	end

	if not exists (
		select 1
		from sys.tables t
		where
			t.object_id = object_id('dbo.Untracked') and
			t.is_tracked_by_cdc = 1
	)
	begin
		exec sys.sp_cdc_enable_table 'dbo', 'Untracked', 'dbo_Untracked', @role_name = null;
	end

	if not exists (
		select 1
		from sys.tables t
		where
			t.object_id = object_id('dbo.UntrackedV2') and
			t.is_tracked_by_cdc = 1
	)
	begin
		exec sys.sp_cdc_enable_table 'dbo', 'UntrackedV2', 'dbo_UntrackedV2', @role_name = null;
	end

	if exists (
		select 1
		from sys.tables t
		where
			t.object_id = object_id('dbo.ShouldBeTracked') and
			t.is_tracked_by_cdc = 1
	)
	begin
		exec sys.sp_cdc_disable_table 'dbo', 'ShouldBeTracked', 'all';
	end

	declare @SourceTable as EasyCDC.SourceTable;
	declare @SourceColumn as EasyCDC.SourceColumn;

	insert into @SourceTable (SchemaName, TableName, CaptureInstancePrefix, SupportsNetChanges, RoleName, IndexName, FilegroupName, AllowPartitionSwitch)
	values ('dbo', 'Tracked', 'dbo_Tracked', 1, null, null, null, 1);

	insert into @SourceColumn (SchemaName, TableName, ColumnName)
	values
		('dbo', 'Tracked', 'Id'),
		('dbo', 'Tracked', 'T1'),
		('dbo', 'Tracked', 'T2');

	insert into @SourceTable (SchemaName, TableName, CaptureInstancePrefix, SupportsNetChanges, RoleName, IndexName, FilegroupName, AllowPartitionSwitch)
	values ('dbo', 'TrackedV2', 'dbo_TrackedV2', 1, null, null, null, 1);

	insert into @SourceColumn (SchemaName, TableName, ColumnName)
	values
		('dbo', 'TrackedV2', 'Id'),
		('dbo', 'TrackedV2', 'T1'),
		('dbo', 'TrackedV2', 'T2');

	exec EasyCDC.MergeConfig @SourceTable, @SourceColumn;

	declare @Consumer as EasyCDC.Consumer;

	insert into @Consumer (ConsumerName)
	values
		('MyConsumer'),
		('AnotherConsumer');

	-- TODO: Merge consumers.
end
go

exec dbo.Setup;
go
exec EasyCDC.ApplyDesiredState @Mode = 'OutputXML';
go

--exec dbo.Setup;
--go
--exec EasyCDC.ApplyDesiredState @Mode = 'OutputString';
--go

exec dbo.Setup;
go
exec EasyCDC.ApplyDesiredState @Mode = 'Commit';
go

--select * from EasyCDC.SourceTable;
--select * from EasyCDC.SourceColumn;
go

--declare @Text nvarchar(max) = N'';
--declare @CrLf nchar(2) = concat(nchar(13), nchar(10));

--with
--	L1(A) as (select 1 union all select 1),
--	L2(A) as (select 1 from L1 a cross apply L1 b),
--	L3(A) as (select 1 from L2 a cross apply L2 b),
--	L4(A) as (select 1 from L3 a cross apply L3 b),
--	L5(A) as (select 1 from L4 a cross apply L4 b)
--select top 10000 @Text += concat('Hello world how are you doing today?', @CrLf)
--from L5;

--select convert(xml, concat('<?Blah -- ', @CrLf, @Text, @CrLf, '-- ?>'));
--go

--exec sys.sp_cdc_disable_table 'dbo', 'A', 'all';
--go
--drop table if exists A;
--go
--create table A (Id int not null primary key);
--go
--exec sys.sp_cdc_enable_table 'dbo', 'A', 'dbo_A', @role_name = null;
--go
