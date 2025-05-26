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
	@SourceName nvarchar(515),
	@TargetName nvarchar(515)
as
begin
	declare
		@SourceServerName sysname = parsename(@SourceName, 4),
		@SourceDatabaseName sysname = parsename(@SourceName, 3),
		@SourceSchemaName sysname = parsename(@SourceName, 2),
		@SourceObjectName sysname = parsename(@SourceName, 1);

	declare
		@TargetServerName sysname = parsename(@TargetName, 4),
		@TargetDatabaseName sysname = parsename(@TargetName, 3),
		@TargetSchemaName sysname = parsename(@TargetName, 2),
		@TargetObjectName sysname = parsename(@TargetName, 1);

	declare
		@SourceNameQualified nvarchar(386) = concat(
			iif(@SourceDatabaseName is not null, concat(@SourceDatabaseName, '.'), ''),
			iif(@SourceSchemaName is not null, concat(@SourceSchemaName, '.'), ''),
			@SourceObjectName
		);

	declare
		@SourceIsTempObjectName bit = case when @SourceObjectName like '#%' then 1 else 0 end,
		@TargetIsTempObjectName bit = case when @TargetObjectName like '#%' then 1 else 0 end,
		@SourceTempDbObjectId int = object_id(concat('tempdb..', @SourceObjectName)),
		@TargetTempDbObjectId int = object_id(concat('tempdb..', @TargetObjectName));

	if @SourceServerName is not null throw 50000, N'@SourceName that specifies a server name part is not supported.', 1;
	if @TargetServerName is not null throw 50000, N'@TargetName that specifies a server name part is not supported.', 1;

	if @SourceIsTempObjectName = 1
	begin
		if @SourceDatabaseName is not null throw 50000, N'@SourceName must not specify database name part when referring to #Temp or ##GlobalTemp objects.', 1;
		if @SourceSchemaName is not null throw 50000, N'@SourceName must not specify schema name part when referring to #Temp or ##GlobalTemp objects.', 1;
	end

	if @TargetIsTempObjectName = 1
	begin
		if @TargetDatabaseName is not null throw 50000, N'@TargetName must not specify database name part when referring to #Temp or ##GlobalTemp objects.', 1;
		if @TargetSchemaName is not null throw 50000, N'@TargetName must not specify schema name part when referring to #Temp or ##GlobalTemp objects.', 1;
	end

	if @SourceIsTempObjectName = 1 and @SourceTempDbObjectId is null throw 50000, N'@SourceName refers to a #Temp or ##GlobalTemp object that does not exist.', 1;
	if @TargetIsTempObjectName = 1 and @TargetTempDbObjectId is null throw 50000, N'@TargetName refers to a #Temp or ##GlobalTemp object that does not exist.', 1;

	select
		@SourceServerName,
		@SourceDatabaseName,
		@SourceSchemaName,
		@SourceObjectName,
		@SourceIsTempObjectName,
		@SourceTempDbObjectId,
		@SourceNameQualified;

	select
		@TargetServerName,
		@TargetDatabaseName,
		@TargetSchemaName,
		@TargetObjectName,
		@TargetIsTempObjectName,
		@TargetTempDbObjectId;

	return 0;
end

go

drop table if exists #Temp, ##Temp;
go
create table #Temp (C1 int);
go
create table ##Temp (C1 int);
go

exec sp_Merge 'A.B.C', '#Temp';
go

