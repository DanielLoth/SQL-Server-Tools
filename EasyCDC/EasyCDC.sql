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

--select * from sys.databases where database_id = db_id();
go

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
go

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
go

drop procedure if exists
	EasyCDC.ConfigureCDC,
	EasyCDC.MergeConfig,
	EasyCDC.UpdateConsumerProgress;
go

drop table if exists
	EasyCDC.SourceColumn,
	EasyCDC.SourceTable,
	EasyCDC.Config;
go

drop trigger if exists DatabaseTrigger on database;
go

drop type if exists	EasyCDC.SourceTable;
drop type if exists EasyCDC.SourceColumn;
go

drop schema if exists EasyCDC;
go

drop user if exists EasyCDCExecUser;
go

create schema EasyCDC;
go

-- 1769223154
-- 20260101T155500Z
-- 20260124T025417Z

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
		constraint EasyCDC_Config_DisableCdcOnUnconfiguredTables_DF default 0,
	SuffixKind nvarchar(20) not null
		constraint EasyCDC_Config_SuffixKind_DF default 'ISO8601'
		constraint EasyCDC_Config_SuffixKind_CK
		check (SuffixKind in ('UnixEpoch', 'ISO8601'))
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
	RoleName sysname not null,
	IndexName sysname not null,
	FilegroupName sysname not null,
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

create procedure EasyCDC.ConfigureCDC
	@Mode nvarchar(20) = 'Commit'
with execute as 'EasyCDCExecUser'
as
begin
	set nocount, xact_abort on;

	if @@trancount != 0 throw 20240520, N'Open transaction not allowed.', 1;
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

	primary key clustered (SchemaName, TableName)
);
go

create type EasyCDC.SourceColumn as table (
	SchemaName sysname not null,
	TableName sysname not null,
	ColumnName sysname not null
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
		from EasyCDC.SourceTable;

		insert into EasyCDC.SourceColumn (
			SchemaName,
			TableName,
			ColumnName
		)
		select
			SchemaName,
			TableName,
			ColumnName
		from EasyCDC.SourceColumn;

		--select d1.*, d2.*
		--from sys.tables t
		--outer apply (select t.* for json path, include_null_values, without_array_wrapper) d1(JsonVal)
		--outer apply (select * from openjson(JsonVal) with (name sysname '$.name')) d2(TableName);

		select a.*
		into #Temp
		from (
			select
				object_schema_name(t.object_id) as SchemaName,
				t.name as TableName,
				t.is_tracked_by_cdc as IsTrackedByCDC
			from sys.tables t
		) t
		outer apply (
			select *
			from (select convert(varchar(20), 'TABLE')) a(RowType)
			outer apply (select t.* for json path, include_null_values, without_array_wrapper) d1(JsonVal)
		) a
		union all
		select a.*
		from (
			select
				object_schema_name(i.object_id) as SchemaName,
				object_name(i.object_id) as ObjectName,
				i.name as IndexName,
				i.is_unique as IsUnique,
				i.is_primary_key as IsPrimaryKey,
				i.is_unique_constraint as IsUniqueConstraint
			from sys.indexes i
			where
				exists (
					select 1
					from sys.tables t
					where
						i.object_id = t.object_id
				)
		) i
		outer apply (
			select *
			from (select convert(varchar(20), 'INDEX')) a(RowType)
			outer apply (select i.* for json path, include_null_values, without_array_wrapper) d1(JsonVal)
		) a;

		--exec tempdb..sp_help '#Temp';

		--select * from sys.indexes;
		select * from cdc.change_tables ct;
		select * from sys.key_constraints;

		commit;
	end try
	begin catch
		if @@trancount != 0 rollback;
		throw;
	end catch
end
go

exec EasyCDC.MergeConfig;
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
