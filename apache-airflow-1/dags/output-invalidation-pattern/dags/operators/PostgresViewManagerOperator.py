from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.decorators import apply_defaults


class PostgresViewManagerOperator(PostgresOperator):
    @apply_defaults
    def __init__(self, view_name, *args, **kwargs):
        super(PostgresViewManagerOperator, self).__init__(*args, **kwargs)
        self.view_name = view_name

    def execute(self, context):
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id, schema=self.database)
        view_tables = hook.get_records(self.sql)
        if view_tables:
            view_table_names = map(lambda tuple: 'SELECT * FROM {}'.format(tuple[0]), view_tables)
            view_tables_union = ' UNION ALL '.join(view_table_names)
            view_query = 'CREATE OR REPLACE VIEW {view_name} AS ({tables})'.format(
                view_name=self.view_name, tables=view_tables_union)
            hook.run(view_query)
        else:
            hook.run('DROP VIEW {}'.format(self.view_name))
