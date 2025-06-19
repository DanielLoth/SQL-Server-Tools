create or alter procedure #GetKeyConstraintBaseline
as
begin
	set nocount, xact_abort on;

	select
		concat(
			quotename(object_schema_name(kc.parent_object_id)),
			'.',
			quotename(object_name(kc.parent_object_id))
		) as ParentTable,
		kc.type as ConstraintType,
		i.type_desc as IndexType,
		convert(nvarchar(max), '') as KeyColumns,
		concat(
			'IsPublished=', kc.is_published, ' ',
			'IsSchemaPublished=', kc.is_schema_published, ' ',
			'IsSystemNamed=', kc.is_system_named, ' ',
			'IsEnforced=', kc.is_enforced
		) as ExtraDetails,
		i.*
	into #KeyConstraint
	from sys.key_constraints kc
	join sys.indexes i on kc.parent_object_id = i.object_id and kc.unique_index_id = i.index_id
	where kc.is_ms_shipped = 0;

	declare @i int = 1;
	declare @MaxIndexColumnId int = (
		select max(ic.index_column_id)
		from sys.index_columns ic
		join sys.indexes i on ic.object_id = i.object_id
		join sys.objects o on i.object_id = o.object_id
		where o.is_ms_shipped = 0
	);

	while @i <= @MaxIndexColumnId
	begin
		update t
		set
			t.KeyColumns += concat(
				case when len(t.KeyColumns) > 0 then ' :: ' else '' end,
				quotename(c.name)
			)
		output 'output' as C1, @i as i, inserted.*
		from #KeyConstraint t
		join sys.indexes i on t.object_id = i.object_id
		join sys.index_columns ic on
			i.object_id = ic.object_id and
			i.index_id = ic.index_id and
			ic.index_column_id = @i
		join sys.columns c on
			ic.object_id = c.object_id and
			ic.column_id = c.column_id
		where
			i.object_id in (select object_id from sys.objects where is_ms_shipped = 0);

		set @i += 1;
	end

	select *
	from #KeyConstraint;
end

go

create or alter procedure #GetForeignKeyBaseline
as
begin
	set nocount, xact_abort on;

	select
		object_id as FkObjectId,
		concat(
			quotename(object_schema_name(parent_object_id)),
			'.',
			quotename(object_name(parent_object_id))
		) as ParentTable,
		concat(
			quotename(object_schema_name(referenced_object_id)),
			'.',
			quotename(object_name(referenced_object_id))
		) as ReferencedTable,
		name as ForeignKeyName,
		convert(nvarchar(max), N'') as ParentColumns,
		convert(nvarchar(max), N'') as ReferencedColumns,
		concat(
			'Disabled=', is_disabled, ' ',
			'NotForRepl=', is_not_for_replication, ' ',
			'IsNotTrusted=', is_not_trusted, ' ',
			'DeleteAction=', delete_referential_action_desc, ' ',
			'UpdateAction=', update_referential_action_desc, ' ',
			'SystemNamed=', is_system_named
		) as ExtraDetails
	into #ForeignKey
	from sys.foreign_keys;

	declare @i int = 1;
	declare @MaxFkColumnId int = (select max(fkc.constraint_column_id) from sys.foreign_key_columns fkc);

	while @i <= @MaxFkColumnId
	begin
		update t
		set
			t.ParentColumns += concat(
				case when len(t.ParentColumns) > 0 then ' :: ' else '' end,
				quotename(c.name), ' ',
				'(',
				'type=', type_name(c.user_type_id), ' ',
				'precision=', isnull(c.precision, '<null>'), ' ',
				'scale=', isnull(c.scale, '<null>'), ' ',
				'maxlen=', isnull(c.max_length, '<null>'),
				')'
			)
		from #ForeignKey t
		join sys.foreign_key_columns fkc on t.FkObjectId = fkc.constraint_object_id and constraint_column_id = @i
		join sys.columns c on fkc.parent_object_id = c.object_id and fkc.parent_column_id = c.column_id;

		update t
		set
			t.ReferencedColumns += concat(
				case when len(t.ReferencedColumns) > 0 then ' :: ' else '' end,
				quotename(c.name), ' ',
				'(',
				'type=', type_name(c.user_type_id), ' ',
				'precision=', isnull(c.precision, '<null>'), ' ',
				'scale=', isnull(c.scale, '<null>'), ' ',
				'maxlen=', isnull(c.max_length, '<null>'),
				')'
			)
		from #ForeignKey t
		join sys.foreign_key_columns fkc on t.FkObjectId = fkc.constraint_object_id and constraint_column_id = @i
		join sys.columns c on fkc.referenced_object_id = c.object_id and fkc.referenced_column_id = c.column_id;

		set @i += 1;
	end

	select
		concat('ParentTable=', ParentTable) as ParentTable,
		concat('ForeignKeyName=', ForeignKeyName) as ForeignKeyName,
		concat('ReferencedTable=', ReferencedTable) as ReferencedTable,
		concat('ParentColumns=', ParentColumns) as ParentColumns,
		concat('ReferencedColumns=', ReferencedColumns) as ReferencedColumns,
		concat('OtherDetails=', ExtraDetails) as ExtraDetails
	from #ForeignKey
	order by
		ParentTable,
		-- TODO
		ReferencedTable;
end

go

drop table if exists A;
go
create table A (Id int constraint PK primary key, C2 int not null, V nvarchar(10) not null unique references A (V));
go
alter table A add constraint FK0 foreign key (Id) references A (Id);
go
alter table A add constraint FK1 foreign key (Id) references A (Id);
go
alter table A add constraint UQ1 unique (Id);
go
alter table A add constraint FK2 foreign key (Id) references A (Id);
go
alter table A add constraint FK3 foreign key (Id) references A (Id);
go
alter table A add constraint UQ2 unique (Id, C2), unique (C2, Id);
go
alter table A add constraint FK4 foreign key (Id, C2) references A (Id, C2), foreign key (Id, C2) references A (C2, Id), foreign key (C2, Id) references A (Id, C2);
go

exec #GetKeyConstraintBaseline;
exec #GetForeignKeyBaseline;
go


select
	t.name as TableName,
	c.name as ColumnName
from sys.tables t
join sys.columns c on t.object_id = c.object_id
where t.is_ms_shipped = 0
order by t.name
for json auto, include_null_values;
