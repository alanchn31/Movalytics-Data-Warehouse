from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_names=[""],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_names = table_names

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for table in self.table_names:
            # Check that entries are being copied to table
            records = redshift.get_records("SELECT COUNT(*) FROM {}".format(table))
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError("Data quality check failed. {} returned no results".format(table))

        # Check that there are no rows with null ids
        dq_checks=[
            {'table': 'movies.movies',
             'check_sql': "SELECT COUNT(*) FROM movies.movies WHERE movie_id is null",
             'expected_result': 0},
            {'table': 'movies.genre',
             'check_sql': "SELECT COUNT(*) FROM movies.genre WHERE genre_id is null",
             'expected_result': 0},
             {'table': 'movies.date',
             'check_sql': "SELECT COUNT(*) FROM movies.date WHERE release_date is null",
             'expected_result': 0},
             {'table': 'movies.cpi',
             'check_sql': "SELECT COUNT(*) FROM movies.cpi WHERE date_cd is null",
             'expected_result': 0},
        ]
        for check in dq_checks:
             records = redshift.get_records(check['check_sql'])
             if records[0][0] != check['expected_result']:
                print("Number of rows with null ids: ", records[0][0])
                print("Expected number of rows with null ids: ", check['expected_result'])
                raise ValueError("Data quality check failed. {} contains null in id column".format(check['table']))