create or alter procedure sp_Semaphore
    @SemaphoreName sysname,
	@Permits int,
    @LockOwner varchar(32) = 'Session',
    @LockTimeoutMilliseconds int = -1,
    @DbPrincipal sysname = N'public',
    @LockLoopAttemptsBeforeDelay int = 5,
    @LockLoopDelayMicroseconds int = 200,
    @AcquiredLockName nvarchar(255) output
as
begin
    set nocount on;

    if @LockOwner not in ('Session', 'Transaction') throw 50000, N'@LockOwner must be one of: ''Session'', ''Transaction''.', 1;
    if @Permits < 1 or @Permits > 10 throw 50000, N'@Permits must be within range: 1 <= @Permits <= 10.', 1;

    declare @i bigint = 1;
    declare @LockName nvarchar(255);
    declare @LockNumericSuffix int;
    declare @StartTime datetime2 = getutcdate();
    declare @Delay datetime = dateadd(microsecond, @LockLoopDelayMicroseconds, convert(datetime, 0x0));

    /*
    As per https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-getapplock-transact-sql?view=sql-server-ver16#return-code-values
    */
    declare @GetAppLockResult int = -1;

    if @LockTimeoutMilliseconds = -1
    begin
        /* Optimistic loop does a first pass through all permits */
        set @i = 0;
        while @i < @Permits
        begin
            set @LockNumericSuffix = @i;
            set @LockName = concat(@SemaphoreName, '-', format(@LockNumericSuffix, 'D2'));
            set @i += 1;

            exec @GetAppLockResult = sp_getapplock
                @Resource = @LockName,
                @LockMode = 'Exclusive',
                @LockOwner = @LockOwner,
                @LockTimeout = 0,
                @DbPrincipal = @DbPrincipal;

            if @GetAppLockResult in (0, 1)
            begin
                return @GetAppLockResult;
            end
        end

        set @i = 0;
        while @GetAppLockResult in (-1, -3)
        begin
            if @i % @LockLoopAttemptsBeforeDelay = 0
            begin
                waitfor delay @Delay;
            end

            set @LockNumericSuffix = @i % @Permits;
            set @LockName = concat(@SemaphoreName, '-', format(@LockNumericSuffix, 'D2'));
            set @i += 1;

            exec @GetAppLockResult = sp_getapplock
                @Resource = @LockName,
                @LockMode = 'Exclusive',
                @LockOwner = @LockOwner,
                @LockTimeout = 0,
                @DbPrincipal = @DbPrincipal;
        end
    end
    else if @LockTimeoutMilliseconds = 0
    begin
        while @i < @Permits and @GetAppLockResult not in (-1, 0)
        begin
            set @LockNumericSuffix = @i % @Permits;
            set @LockName = concat(@SemaphoreName, '-', format(@LockNumericSuffix, 'D2'));
            set @i += 1;

            exec @GetAppLockResult = sp_getapplock
                @Resource = @LockName,
                @LockMode = 'Exclusive',
                @LockOwner = @LockOwner,
                @LockTimeout = 0,
                @DbPrincipal = @DbPrincipal;
        end
    end
    else
    begin
        ;throw 50000, N'Not yet supported', 1;
    end

    declare @ElapsedTime datetime2 = datediff(microsecond, @StartTime, getutcdate());
    set @AcquiredLockName = @LockName;

    if @GetAppLockResult = 0
    begin
        if @ElapsedTime < 500 return 0; /* Less than 500 microseconds, we consider it synchronous */
        else return 1; /* More than 500 microseconds and we assume it wasn't actually synchronous */
    end

	return @GetAppLockResult;
end

go
