set ansi_nulls on;
set ansi_padding on;
set ansi_warnings on;
set arithabort on;
set concat_null_yields_null on;
set quoted_identifier on;
set statistics io off;
set statistics time off;
go

drop procedure if exists sp_AllEffectivePermissions;
go

create or alter procedure sp_AllEffectivePermissions
	@Help bit = 0,
	@Mode sysname = 'report',
	@Debug bit = 1,
	@ReportSchemaNamePrefix nvarchar(20) = N'sp_AEP_',
	@RoleNamePrefix nvarchar(20) = N'sp_AEP_',
	@ModifyObjectsInSingleTransaction bit = 1
as
begin
	set nocount, xact_abort on;

	if @@trancount != 0 throw 20240520, N'Open transaction not allowed.', 1;
	if is_rolemember('db_owner') = 0 throw 20240520, N'Must be a member of the db_owner fixed database role.', 1;
	if @Mode not in ('report', 'cleanup') throw 20240520, N'@Mode must be one of: "report", "cleanup".', 1;
	if object_id('tempdb..#T1') is not null throw 20240520, N'Temporary table #T1 must not exist.', 1;

	--select * from sys.fn_my_permissions(null, 'database');
	--select * from sys.fn_my_permissions(null, 'server');

	declare @NameSuffix varchar(50) = replace(convert(varchar(50), newid()), '-', '');
	declare @ReportSchemaName sysname = concat(@ReportSchemaNamePrefix, @NameSuffix);
	declare @RoleWithConnectName sysname = concat(@RoleNamePrefix, @NameSuffix);

	declare @Query nvarchar(max);

	create table #ReportSchemaQuery (
		Id int not null
			check (Id =  1)
			default 1
			unique,
		SchemaName sysname primary key,
		CreateSchemaQuery as concat('create schema ', quotename(SchemaName), ';'),
		DropSchemaQuery as concat('drop schema if exists ', quotename(SchemaName), ';')
	);

	create table #RoleWithConnectQuery (
		Id int not null
			check (Id = 1)
			default 1
			unique,
		RoleName sysname primary key,
		CreateRoleQuery as concat('create role ', quotename(RoleName), ';'),
		DropRoleQuery as concat('drop role if exists ', quotename(RoleName), ';'),
		GrantConnectQuery as concat('grant connect to ', quotename(RoleName), ';')
	);

	create table #RoleWithConnectAddMemberQuery (
		RoleName sysname not null,
		PrincipalName sysname not null,
		AlterRoleAddMemberQuery as concat('alter role ', quotename(RoleName), ' add member ', quotename(PrincipalName), ';'),
		unique (RoleName, PrincipalName)
	);

	create table #RoleUserProxyQuery (
		RoleName sysname not null,
		--CreateProxyUserQuery as concat('create user ', quotename(RoleName) ';'),
		--DropProxyUserQuery as concat('x', 'x')
	);

	create table #DatabaseUser (
		PrincipalName sysname primary key
	);

	create table #DatabaseRole (
		PrincipalName sysname primary key,
		ProxyUserName sysname unique
	);

	create table #ExecuteAsPrincipal (
		RoleWithConnectName sysname,
		ExecuteAsPrincipalName sysname primary key
	);

	insert into #ReportSchemaQuery (SchemaName) values (@ReportSchemaName);
	insert into #RoleWithConnectQuery (RoleName) values (@RoleWithConnectName);

	insert into #DatabaseUser (PrincipalName)
	select dp.name
	from sys.database_principals dp
	where
		dp.type_desc = 'SQL_USER' and
		dp.name not in (
			'INFORMATION_SCHEMA'
		);

	insert into #DatabaseRole (PrincipalName, ProxyUserName)
	select
		dp.name,
		convert(sysname, substring(concat(@RoleNamePrefix, dp.name), 1, 128))
	from sys.database_principals dp
	where
		dp.is_fixed_role = 0 and
		dp.type_desc = 'DATABASE_ROLE' and
		dp.name not in (
			'public',
			'sys'
		);

	insert into #ExecuteAsPrincipal (RoleWithConnectName, ExecuteAsPrincipalName)
	select
		@RoleWithConnectName,
		s.PrincipalName
	from (
		select a.PrincipalName
		from #DatabaseUser a
		union all
		select a.ProxyUserName
		from #DatabaseRole a
	) s;

	if @Debug = 1
	begin
		select '' as [#ReportSchemaQuery], * from #ReportSchemaQuery;
		select '' as [#DatabaseUser], * from #DatabaseUser;
		select '' as [#DatabaseRole], * from #DatabaseRole;
		select '' as [#RoleUserProxyQuery], * from #RoleUserProxyQuery;
		select '' as [#RoleWithConnectAddMemberQuery], * from #RoleWithConnectAddMemberQuery;
		select '' as [#RoleWithConnectQuery], * from #RoleWithConnectQuery;
		select '' as [#ExecuteAsPrincipal], * from #ExecuteAsPrincipal;
	end

	begin try
		if @ModifyObjectsInSingleTransaction = 1
		begin
			begin transaction;
		end

		set @Query = (select CreateSchemaQuery from #ReportSchemaQuery);
		exec(@Query);

		set @Query = (select CreateRoleQuery from #RoleWithConnectQuery);
		exec(@Query);

		set @Query = (select GrantConnectQuery from #RoleWithConnectQuery);
		exec(@Query);

		--select * from sys.database_principals;

		begin try
			declare PrincipalCursor cursor local fast_forward for
				select AlterRoleAddMemberQuery
				from #RoleWithConnectAddMemberQuery;

			open PrincipalCursor;
			fetch next from PrincipalCursor into @Query;

			while @@fetch_status = 0
			begin
				exec(@Query);
				fetch next from PrincipalCursor into @Query;
			end
		end try
		begin catch
			;throw;
		end catch

		if @ModifyObjectsInSingleTransaction = 1
		begin
			rollback;
		end
	end try
	begin catch
		if @@trancount != 0 rollback;
		throw;
	end catch
end

go

exec sp_AllEffectivePermissions;
go
--exec sp_AllEffectivePermissions;
go
