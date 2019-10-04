from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.decorators import apply_defaults


class PostgresCopyOperator(PostgresOperator):
    template_fields = ('source_file', 'target_table')

    @apply_defaults
    def __init__(self, source_file, target_table, *args, **kwargs):
        super(PostgresCopyOperator, self).__init__(sql='x', *args, **kwargs)
        self.source_file = source_file
        self.target_table = target_table

    def execute(self, context):
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id, schema=self.database)
        hook.bulk_load(table=self.target_table, tmp_file=self.source_file)
