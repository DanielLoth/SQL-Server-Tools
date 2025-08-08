/*
sp_SuperviseMe: A tool for SQL Server database administrators who want to run
operations on their database without the risk of unknowingly blocking other
workloads that are running.

Install it in your master database so you can run it anywhere.

Written by Daniel Loth.
*/
go
create or alter procedure sp_SuperviseMe
	@WaitTimeThresholdMilliseconds int = 1000,
	@DelayBetweenChecksMilliseconds int = 500,
	@DelayBetweenChecksAfterKillMilliseconds int = 250,
	@SupervisorMinutes int = 120,
	@PrintKills bit = 0,
	@PrintDelays bit = 0,
	@Help bit = 0
as
begin
	set nocount, xact_abort on;

	if @Help = 1
	begin
		raiserror(N'
/*
sp_SuperviseMe

Written by Daniel Loth.

---

Why use this tool?

Certain operations - for example DROP INDEX - are not performed online.
Not even when using SQL Server Enterprise Edition.
In a busy SQL Server database, such an operation will block other users of the
database while they wait for the index to be dropped.
An index drop might not happen instantly though. For example, there might be
a long-running read query that - while otherwise harmless to other users - is
blocking your DROP INDEX operation.

This tool is intended as a safety net for those who intend to perform such
operations.

Please note that this tool will monitor for, and kill, other sessions that are
running under the same login as this procedure is executed under.

In its current form it is intended to be used as a self-supervision tool.
You might perhaps run it in one tab in SQL Server Management Studio and then
run your to-be-supervised query in another tab that is connected to SQL Server
using the same login.

---

Permission requirements.

You must have the following permissions:
- VIEW SERVER STATE (to read SQL Server metadata from its DMVs).
- ALTER ANY CONNECTION (to issue KILL commands).

---

Parameters:

@WaitTimeThresholdMilliseconds - Your session must block others for at least
this long to be deemed killable.

@DelayBetweenChecksMilliseconds - Cooldown period between checks when no kill
command issued.

@DelayBetweenChecksAfterKillMilliseconds - Cooldown period after a check that
concluded with killing a session.

@SupervisorMinutes - The number of minutes this supervisor procedure will run
for.

@PrintKills - Whether or not to print messages when kills occur.

@PrintDelays - Whether or not to print messages when a delay between checks
occurs.

@Help - Shows this help information.
', 0, 1) with nowait;

		raiserror(N'
---

Example 1:

exec sp_SuperviseMe
	@WaitTimeThresholdMilliseconds = 2000,
	@DelayBetweenChecksMilliseconds = 500;

This will kill other sessions that you are running if those sessions block other
database sessions for more than 2 seconds at a time.
It will wait 500 milliseconds between performing each check, so at most will
query the SQL Server metadata views for the necessary information twice per
second.

The tool will provide output similar to the following:
--------------------------------------------------------------------------------
sp_SuperviseMe is now running.
Start time (UTC): 2025-08-06T13:55:59.877+00:00
Scheduled end time (UTC): 2025-08-06T13:56:59.877+00:00
Supervised login name: DESKTOP-HL4PF9Q\dan
--------------------------------------------------------------------------------

*/
', 0, 1) with nowait;

		return 0;
	end

	if @@trancount != 0
	begin
		;throw 502913, N'Open transaction not allowed.', 1;
	end

	if has_perms_by_name(null, null, 'view server state') != 1
	begin
		;throw 502913, N'Must have VIEW SERVER STATE permission.', 1;
	end

	if has_perms_by_name(null, null, 'alter any connection') != 1
	begin
		;throw 502913, N'Must have ALTER ANY CONNECTION permission.', 1;
	end

	declare @Msg nvarchar(1000);
	declare @SessionId smallint;
	declare @LoginTime datetime;
	declare @FoundKillTarget bit;
	declare @StartUtcDtm datetimeoffset(3) = getutcdate() at time zone 'UTC';
	declare @EndUtcDtm datetimeoffset(3) = dateadd(minute, @SupervisorMinutes, getutcdate()) at time zone 'UTC';
	declare @Query nvarchar(1000);

	declare @DelayBetweenChecks datetime = dateadd(millisecond, @DelayBetweenChecksMilliseconds, convert(datetime, 0x0));
	declare @DelayAfterKill datetime = dateadd(millisecond, @DelayBetweenChecksAfterKillMilliseconds, convert(datetime, 0x0));
	declare @Delay datetime;

	declare @SupervisedLoginName sysname = suser_sname();
	declare @AppLockResourceName nvarchar(255) = concat('sp_SuperviseMe-', @SupervisedLoginName);
	declare @AppLockResult int;

	raiserror(N'--------------------------------------------------------------------------------', 0, 1) with nowait;
	raiserror(N'sp_SuperviseMe is now running.', 0, 1) with nowait;

	set @Msg = concat('Start time (UTC): ', convert(varchar(50), @StartUtcDtm, 126));
	raiserror(@Msg, 0, 1) with nowait;

	set @Msg = concat('Scheduled end time (UTC): ', convert(varchar(50), @EndUtcDtm, 126));
	raiserror(@Msg, 0, 1) with nowait;

	set @Msg = concat('Supervised login name: ', @SupervisedLoginName);
	raiserror(@Msg, 0, 1) with nowait;

	raiserror(N'--------------------------------------------------------------------------------', 0, 1) with nowait;

	create table #BlockedSessionKillList (
		SessionId smallint not null,
		LoginTime datetime not null,
		primary key (SessionId, LoginTime)
	);

	while getutcdate() < @EndUtcDtm
	begin
		begin try
			set @FoundKillTarget = 0;
			set @SessionId = null;
			set @LoginTime = null;
			set @Query = null;
			set @AppLockResult = null;

			with
				MySessions as (
					select
						s.session_id as SessionId,
						s.login_time as LoginTime
					from sys.dm_exec_sessions s
					where
						s.original_login_name = @SupervisedLoginName and
						s.is_user_process = 1 and
						not exists (
							select 1
							from #BlockedSessionKillList b
							where
								s.session_id = b.SessionId and
								s.login_time = b.LoginTime
						)
				),
				BlockedByMySessions as (
					select
						session_id as BlockedSessionId,
						blocking_session_id as BlockingSessionId,
						wait_time as BlockedSessionCurrentWaitTimeMilliseconds
					from sys.dm_exec_requests
					where
						blocking_session_id in (select SessionId from MySessions) and
						wait_time >= @WaitTimeThresholdMilliseconds
				)
			select top 1
				@FoundKillTarget = 1,
				@SessionId = BlockingSessionId,
				@LoginTime = BlockingSessionLoginTime
			from BlockedByMySessions a
			outer apply (
				select
					b.LoginTime as BlockingSessionLoginTime
				from MySessions b
				where
					a.BlockingSessionId = b.SessionId
			) d1;

			if @FoundKillTarget = 1
			begin
				begin try
					exec @AppLockResult = master..sp_getapplock
						@Resource = @AppLockResourceName,
						@LockMode = 'Exclusive',
						@LockOwner = 'Session',
						@LockTimeout = 10;

					set @AppLockResult = iif(@AppLockResult in (0,1), 0, null);

					if @AppLockResult = 0
					begin
						if exists (
							select 1
							from sys.dm_exec_sessions
							where
								session_id = @SessionId and
								login_time = @LoginTime
						)
						begin
							set @Query = concat('kill ', @SessionId, ';');
							exec sp_executesql @stmt = @Query;
						end

						begin try
							while @AppLockResult = 0
							begin
								exec @AppLockResult = master..sp_releaseapplock
									@Resource = @AppLockResourceName,
									@LockOwner = 'Session';
							end
						end try
						begin catch
							if error_number() != 1223 throw;
						end catch
					end
				end try
				begin catch
					if @@trancount != 0 rollback;

					begin try
						while @AppLockResult = 0
						begin
							exec @AppLockResult = master..sp_releaseapplock
								@Resource = @AppLockResourceName,
								@LockOwner = 'Session';
						end
					end try
					begin catch
						if error_number() != 1223 throw;
					end catch

					if error_number() not in (
						-- Process ID %d is not an active process ID.
						6106,

						/*
						SPID %d: transaction rollback in progress.
						Estimated rollback completion: %d%%.
						Estimated time remaining: %d seconds.
						*/
						6109
					)
					begin
						;throw;
					end
				end catch

				if @PrintKills = 1
				begin
					raiserror(@Query, 0, 1) with nowait;
				end

				insert into #BlockedSessionKillList (SessionId, LoginTime)
				values (@SessionId, @LoginTime);
			end
			else
			begin
				delete top (100) a
				from #BlockedSessionKillList a
				where
					not exists (
						select 1
						from sys.dm_exec_sessions s
						where
							a.SessionId = s.session_id and
							a.LoginTime = s.login_time
					);
			end

			set @Delay = iif(@FoundKillTarget = 1, @DelayAfterKill, @DelayBetweenChecks);

			if @PrintDelays = 1
			begin
				set @Msg = concat('Delay: ', datepart(millisecond, @Delay), ' milliseconds.');
				raiserror(@Msg, 0, 1) with nowait;
			end

			waitfor delay @Delay;
		end try
		begin catch
			if @@trancount != 0 rollback;

			set @Msg = concat(
				'Unexpected error: ',
				'Error number = ', error_number(), ' - ',
				'Error message = ', error_message()
			);
			raiserror(@Msg, 0, 1) with nowait;

			waitfor delay @DelayBetweenChecks;
		end catch
	end

	raiserror('sp_SuperviseMe has now stopped.', 0, 1) with nowait;

	return 0;
end
go

exec sp_SuperviseMe @WaitTimeThresholdMilliseconds = 2000, @SupervisorMinutes = 1;
go


--exec master..sp_releaseapplock
--	@Resource = 'sp_SuperviseMe-DESKTOP-HL4PF9Q\dan',
--	@LockOwner = 'Session';

/*
Test query

if @@trancount != 0 rollback;
begin transaction;
select count(1) from dbo.MyTable with (serializable, tablockx);
waitfor delay '01:00:00.000';
*/
