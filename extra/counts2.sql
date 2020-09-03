copy (
    with
    counts as (
        select
            table_catalog,
            table_schema,
            table_name,
            table_type,
            (xpath(
                '/row/table_count/text()',
                xml_count
            ))[1]::text::int as table_count
        from (
            select 
                table_catalog,
                table_schema,
                table_name,
                table_type,
                query_to_xml(
                    format(
                        '
                        select count(*) as table_count
                        from %I.%I.%I
                        ', 
                        table_catalog,
                        table_schema,
                        table_name
                    ), 
                    false, 
                    true,
                    ''
                ) as xml_count
            from information_schema.tables
        ) as t
    ),
    hashes as (
        select
            table_catalog,
            table_schema,
            table_name,
            table_type,

            (xpath(
                '/row/table_hash/text()',
                xml_hash
            ))[1]::text as table_hash
        from (
            select
                table_catalog,
                table_schema,
                table_name,
                table_type,
                query_to_xml(
                    format(
                        '
                        select md5(coalesce(string_agg(x, '','' order by x), '''')) as table_hash
                        from (
                            select md5(coalesce(string_agg(x, '','' order by x), '''')) as x
                            from (select md5(row(f.*)::text) as x from %I.%I.%I as f) as t
                            group by substring(x from 1 for 2)
                        ) as t2
                        ',
                        table_catalog,
                        table_schema,
                        table_name
                    ), 
                    false, 
                    true,
                    ''
                ) as xml_hash
            from information_schema.tables
        ) as t
    )
    select
        'replaceme' table_node,
        n.table_catalog,
        n.table_schema,
        n.table_name,
        n.table_type,
        h.table_hash,
        n.table_count
    from counts n join hashes h using (table_catalog, table_schema, table_name)
    where n.table_schema = 'public' and n.table_type = 'BASE TABLE'
) to stdout with csv header;
