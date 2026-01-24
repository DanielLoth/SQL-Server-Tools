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

drop procedure if exists
	EasyCDC.P1,
	EasyCDC.ConfigureCDC;
go

drop table if exists
	EasyCDC.SourceTable,
	EasyCDC.SourceColumn,
	EasyCDC.Config;
go

drop trigger if exists DatabaseTrigger on database;
go

drop type if exists EasyCDC.MyType;
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
	primary key clustered (SchemaName, TableName, ColumnName)
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
as
begin
	set nocount, xact_abort on;
end
go

create type EasyCDC.MyType as table (
	Id int
);
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
