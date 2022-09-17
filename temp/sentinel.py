import boto3
import athena_util
import pandas as pd

class Sentinel():
    def __init__(self):
        self.params = {
            'region': 'us-east-1',
            'database': 'life360_reference',
            'bucket': 'prod-life360-athena-query-results-us-east-1',
            'path': ''
        }
        self.glue_client = boto3.client('glue', region_name=self.params['region'])
        self.s3_client = boto3.client('s3', region_name=self.params['region'])
        self.session = boto3.Session()
        self.get_databases()

    def fetch_row_counts(self):
        self.params['query'] = f"""SELECT 
                                    upload_dt,
                                    count(1) as total_count FROM db_event_types.session_active_start 
                                    WHERE upload_dt >= '2020-09-07'
                                    AND upload_dt <= '2020-09-15'
                                    GROUP BY 1
                                """
        location, data = athena_util.query_results(self.session, self.params)

        return data

    def run_athena_query(self, query):
        self.params['query'] = query
        location, data = athena_util.query_results(self.session, self.params)
        return data

    def parse_athena_results(self, results):
        if 'Rows' not in results:
            return []
        data = results['Rows']
        output = []
        for i, row in enumerate(data):
            column_output = []
            for j, column in enumerate(row['Data']):
                if 'VarCharValue' in column:
                    column_output.append(column['VarCharValue'])
            output.append(column_output)
        return output

    def get_max_partition(self, table_name):
        pass

    def get_table_metadata(self, database, table_name):
        response = self.glue_client.get_table(
            DatabaseName=database,
            Name=table_name
        )
        return response['Table']

    def get_varchar_values(self, row_object, cast_to=None):
        if cast_to == 'int':
            return [int(x['VarCharValue']) for x in row_object]
        return [x['VarCharValue'] for x in row_object]

    def get_databases(self):
        databases = self.glue_client.get_databases(CatalogId='102611674515')
        self.database_names = [db['Name'] for db in databases['DatabaseList']]

    def check_nulls(self, database, table_name):
        metadata = self.get_table_metadata(database, table_name)
        sql_rendered = ['SUM(CASE WHEN ' + x['Name'] + ' IS NULL THEN 1 ELSE 0 END) ' + 'as ' + x['Name'] + '_null_cnt,' for x in metadata['StorageDescriptor']['Columns'] ]
        sql_block_string = '\n'.join(sql_rendered) + '\nCOUNT(1) as total_records'

        sql_query = 'SELECT\n' + sql_block_string + f'\nFROM {database}.{table_name};'
        print(sql_query)
        data = self.run_athena_query(sql_query)['Rows']
        headers = self.get_varchar_values(data[0]['Data'])
        dataset = self.get_varchar_values(data[1]['Data'], 'int')
        metadata = dict(zip(headers, dataset))
        total_records = metadata['total_records']
        output_array = [[k, v] for k, v in metadata.items()]
        df = pd.DataFrame(output_array, columns=['Measure', 'Value'])
        # df = pd.DataFrame(output_array, columns=['Measure', 'Value'])
        df['Percent'] = df['Value'] / total_records
        print(df.sort_values(by=['Percent'], ascending=False))




def main():
    sentinel = Sentinel()
    #sentinel.check_nulls('das_stg','circle_d')
    partitions = sentinel.run_athena_query('SHOW PARTITIONS prod_amplitude_events.amplitude_events')
    data = sentinel.parse_athena_results(partitions)

if __name__ == "__main__":
    main()