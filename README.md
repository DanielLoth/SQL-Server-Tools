# SQL-Server-Tools
<a name="header1"></a>
[![licence badge]][licence]
[![issues badge]][issues]

## sp_Semaphore

The `sp_Semaphore` is intended to facilitate a situation in which you only want `N` executions of something to occur simultaneously.

For example, if you have a SQL Server that hosts multiple databases you might wish to:
- Limit the number of simultaneous index rebuilds to 2
- Limit the number of simultaneous `sqlpackage` deployments to 2.

This tool can help with that.

It's behaviour is much the same as the `SemaphoreSlim` class within the .NET standard library, and this implementation is built on top of the `sp_getapplock` stored procedure that ships with SQL Server.

[licence badge]:https://img.shields.io/badge/license-MIT-green.svg
[issues badge]:https://img.shields.io/github/issues/DanielLoth/SQL-Server-Tools.svg

[licence]:https://github.com/DanielLoth/SQL-Server-Tools/blob/master/LICENSE
[issues]:https://github.com/DanielLoth/SQL-Server-Tools/issues
