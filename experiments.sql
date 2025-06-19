set nocount, xact_abort on;
go
set statistics io off;
go
if @@trancount != 0 rollback;
go
drop table if exists A;
go
create table A (
	Id uniqueidentifier not null
		constraint PK primary key,
	C1 uniqueidentifier not null
		constraint U1 unique,
	Filler char(1000) not null
		constraint DF1 default '',
	C2 uniqueidentifier not null
		constraint U2 unique
		constraint DF_A_C2 default newid()
		constraint FK_A_C2_ColumnConstraint_SelfRef references A (C2)
		constraint CK_A_C2_ColumnConstraint check (C2 is not null),

	constraint FK_A_C2_TableConstraint_SelfRef
	foreign key (C2) references A (C2),

	constraint CK_A_C2_TableConstraint check (C2 is not null and C1 is not null),
	constraint U3 unique (C1),

	constraint FK1 foreign key (C2) references A (C2)
);
go
insert into A (Id, C1)
select top 1000 newid(), newid()
from sys.all_columns;
go

--select * from sys.indexes where object_id = object_id('A');
go
--alter table A drop constraint U1;
set statistics io on;
go

begin transaction;
--alter table A drop constraint FK1, PK, U1;

exec sp_rename @objname = N'dbo.A', @newname = N'A', @objtype = 'object';
exec sp_rename @objname = N'dbo.FK1', @newname = N'FK1', @objtype = 'object';

select * from sys.objects where object_id in (object_id('dbo.A'), object_id('dbo.FK1'));


--select * from sys.foreign_keys;
--select * from sys.check_constraints;

--alter table A drop
--	constraint if exists FK1,
--	constraint FK_A_C2_ColumnConstraint_SelfRef,
--	constraint FK_A_C2_TableConstraint_SelfRef,
--	constraint if exists U1,
--	constraint U2,
--	constraint U3,
--	constraint if exists PK;

select * from sys.dm_tran_locks where request_session_id = @@spid and request_mode = 'Sch-M';

commit;
go
--select * from sys.indexes where object_id = object_id('A');
go
--drop index PK on A;
go
