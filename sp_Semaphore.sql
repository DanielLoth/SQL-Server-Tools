set ansi_nulls on;
set ansi_padding on;
set ansi_warnings on;
set arithabort on;
set concat_null_yields_null on;
set quoted_identifier on;
set statistics io off;
set statistics time off;
go

/*
The sp_Semaphore procedure might be useful in the following use-cases.

Use case 1: Limiting the number of index builds on a single node.

Use case 2: Limiting the number of sqlpackage publish executions that run
concurrently against a single node.
To achieve this, run sp_Semaphore during the pre-deployment phase of a
sqlpackage execution.
*/
go

create or alter procedure sp_Semaphore
    @SemaphoreName sysname = N'sp_Semaphore',
	@PermitCount int = 1,
    @LockOwner varchar(32) = 'Session',
    @LockTimeoutMilliseconds int = -1,
    @DbPrincipal sysname = N'public',
    @LockLoopDelayMilliseconds int = 1,
    @AcquiredLockName nvarchar(255) = null output
as
begin
    if @LockOwner not in ('Session', 'Transaction') throw 501900000, N'Parameter @LockOwner must be one of: ''Session'', ''Transaction''.', 1;
    if @PermitCount < 1 or @PermitCount > 10 throw 501900000, N'Parameter @PermitCount must be within range: 1 <= @PermitCount <= 10.', 1;
	if @LockTimeoutMilliseconds < -1 throw 501900000, N'Parameter @LockTimeoutMilliseconds must be within range: -1 <= @LockTimeoutMilliseconds <= 2147483647.', 1;
	if @LockLoopDelayMilliseconds < 0 throw 501900000, N'Parameter @LockLoopDelayMilliseconds must be within range: 0 <= @LockLoopDelayMilliseconds <= 2147483647.', 1;

    declare @i bigint = 0;
    declare @LockName nvarchar(255) = N'';
    declare @Delay datetime = dateadd(millisecond, @LockLoopDelayMilliseconds, convert(datetime, 0x0));
	declare @AllLocksAttempted bit = 0;
	declare @GetAppLockResult int = -1;
	declare @ElapsedTime bigint = 0;
	declare @StartTime datetime2 = getutcdate();

	while @LockTimeoutMilliseconds = -1 or @ElapsedTime < @LockTimeoutMilliseconds
	begin
		set @i = 0;

		while @i < @PermitCount
		begin
			set @LockName = concat(@SemaphoreName, '-', @i);
			set @i += 1;

			exec @GetAppLockResult = sp_getapplock
				@Resource = @LockName,
				@LockMode = 'Exclusive',
				@LockOwner = @LockOwner,
				@LockTimeout = 0,
				@DbPrincipal = @DbPrincipal;

			if @GetAppLockResult in (0, 1)
			begin
				set @AcquiredLockName = @LockName;
				return 0;
			end
			else if @GetAppLockResult = -1
			begin
				if @AllLocksAttempted = 1 and @LockLoopDelayMilliseconds > 0
				begin
					waitfor delay @Delay;
				end

				continue;
			end
			else if @GetAppLockResult = -2 throw 501900002, N'The lock request was cancelled.', 1;
			else if @GetAppLockResult = -3 throw 501900003, N'Lock request was chosen as deadlock victim.', 1;
			else if @GetAppLockResult = -999 throw 501900999, N'Parameter validation or other call error.', 1;
		end

		set @AllLocksAttempted = 1;
		set @ElapsedTime = datediff_big(millisecond, @StartTime, getutcdate());

		if @LockTimeoutMilliseconds = -1
		begin
			continue; /* Infinite loop */
		end

		if @LockTimeoutMilliseconds = 0 or @ElapsedTime >= @LockTimeoutMilliseconds
		begin
			;throw 501900001, N'The lock request timed out.', 1;
		end
	end

	return 0;
end

go
