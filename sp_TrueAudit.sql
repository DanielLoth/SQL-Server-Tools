set ansi_nulls on;
set ansi_padding on;
set ansi_warnings on;
set arithabort on;
set concat_null_yields_null on;
set quoted_identifier on;
set statistics io off;
set statistics time off;
go

create or alter procedure sp_TrueAudit
	@IncludeDatabasePrincipalId bit,
	@DatabasePrincipalIdColumnName sysname,
	@IncludeDatabasePrincipalName bit,
	@DatabasePrincipalNameColumnName sysname,
	@IncludeServerPrincipalId bit,
	@ServerPrincipalIdColumnName sysname,
	@IncludeServerPrincipalName bit,
	@ServerPrincipalNameColumnName sysname
as
begin
	set nocount, xact_abort on;
end

go

create or alter trigger TrueAudit_AlterTable
on database
for alter_table, create_table
as
begin
	select eventdata();
end
go

drop table if exists dbo.MyTable;
go
create table dbo.MyTable (Id int primary key);
go
alter table dbo.MyTable add C1 int null, C2 int null, C3 int null;
go
alter table dbo.MyTable drop column if exists C2, column if exists C3, column if exists C4;
go

drop trigger TrueAudit_AlterTable on database;
go
