primary_keys as (
    select
        t.table_catalog,
        t.table_schema,
        t.table_name,
        string_agg(
            c.column_name, 
            ',' order by c.ordinal_position
        ) as column_names
    from information_schema.key_column_usage as c
        left join information_schema.table_constraints as t
            on t.constraint_name = c.constraint_name
    where t.constraint_type = 'PRIMARY KEY' 
    group by t.table_catalog, t.table_schema, t.table_name
),
columns as (
    select
        table_catalog,
        table_schema,
        table_name,
        string_agg(
            column_name, 
            ',' order by ordinal_position
        ) as column_names
    from information_schema.columns
    group by table_catalog, table_schema, table_name
),
columns_with_primary_keys as (
    select
        c.table_catalog,
        c.table_schema,
        c.table_name,
        case 
            when p.column_names is null
                then '' 
            else p.column_names || ','
        end || c.column_names as column_names
    from columns c left outer join primary_keys p
        using (table_catalog, table_schema, table_name)
)


                                encode(
                                    sha256(
                                        convert_to(
                                            row(f.*)::text,
                                            ''UTF-8''
                                        )
                                    ),
                                    ''hex''
                                ) as x
