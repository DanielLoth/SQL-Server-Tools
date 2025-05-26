# SQL-Server-Tools

[![licence badge]][licence]
[![issues badge]][issues]
[![contributors_badge]][contributors]

## sp_Semaphore

The `sp_Semaphore` is intended to facilitate a situation in which you only want `N` executions of something to occur simultaneously.

For example, if you have a SQL Server that hosts multiple databases you might wish to:
- Limit the number of simultaneous index rebuilds to 2
- Limit the number of simultaneous `sqlpackage` deployments to 2.

This tool can help with that.

It's behaviour is much the same as the `SemaphoreSlim` class within the .NET standard library, and this implementation is built on top of the `sp_getapplock` stored procedure that ships with SQL Server.
