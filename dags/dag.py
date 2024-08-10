from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.mysql_hook import MySqlHook
from datetime import datetime, timedelta
from airflow.utils.trigger_rule import TriggerRule
import pandas as pd

import json
args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'params': {"type": "append"},
    }
def convert_lists_to_string(row):
    return [json.dumps(item) if isinstance(item, list) else item for item in row]

def map_postgres_to_mysql(pg_type):
    """Map PostgreSQL data types to MySQL data types."""
    type_mapping = {
        'integer': 'INT',
        'smallint': 'SMALLINT',
        'bigint': 'BIGINT',
        'serial': 'INT AUTO_INCREMENT',
        'bigserial': 'BIGINT AUTO_INCREMENT',
        'decimal': 'DECIMAL',
        'numeric': 'DECIMAL',
        'real': 'FLOAT',
        'double precision': 'DOUBLE',
        'boolean': 'BOOLEAN',
        'date': 'DATE',
        'timestamp': 'TIMESTAMP',
        'timestamp with time zone': 'TIMESTAMP',
        'text': 'TEXT',
        'character varying': 'VARCHAR',
        'char': 'CHAR',
        'bytea': 'BLOB',
        'ARRAY':'JSON',
        'array':'JSON',
    }
    return type_mapping.get(pg_type, 'LONGTEXT')

get_id = {
    'actor_info': 'actor_id ',
    'customer_list': 'id',
    'film_list': 'fid',
    'nicer_but_slower_film_list': 'fid',
    'staff_list': 'id',
    'payment': 'payment_id',
    'payment_p2022_02': 'payment_id',
    'payment_p2022_03': 'payment_id',
    'payment_p2022_07': 'payment_id',
    'payment_p2022_05': 'payment_id',
    'payment_p2022_06': 'payment_id',
    'payment_p2022_01': 'payment_id',
    'payment_p2022_04': 'payment_id',
    
}

with DAG(
    dag_id='migrate_data_dag',
    default_args=args,
    description='A DAG for batch incremental loading',
    schedule_interval='@daily',  # Run daily at midnight
    start_date=datetime(2024, 8, 6),
) as dag:
          
    def create_mysql_tables(**kwargs):
        """Migrate all tables from PostgreSQL to MySQL."""
        postgres_hook = PostgresHook(postgres_conn_id='postgres_source')
        mysql_hook = MySqlHook(mysql_conn_id='mysql_dest')

        postgres_conn = postgres_hook.get_conn()
        postgres_cursor = postgres_conn.cursor()
        mysql_conn = mysql_hook.get_conn()
        mysql_cursor = mysql_conn.cursor()

        # Get all table names from PostgreSQL
        postgres_cursor.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
        """)
        tables = postgres_cursor.fetchall()

        for table_name_tuple in tables:
            table_name = table_name_tuple[0]
            print(f"Processing table: {table_name}")

            # Get column details
            postgres_cursor.execute(f"""
                SELECT column_name, data_type, 
                    character_maximum_length, numeric_precision, numeric_scale,is_nullable
                FROM information_schema.columns
                WHERE table_name = '{table_name}' AND table_schema = 'public'
            """)
            columns_info = postgres_cursor.fetchall()

            # Get primary key information
            postgres_cursor.execute(f"""
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_name = kcu.table_name
                WHERE tc.table_name = '{table_name}'
                AND tc.constraint_type = 'PRIMARY KEY'
                AND tc.table_schema = 'public'
            """)
            primary_keys = postgres_cursor.fetchall()

            primary_key_columns = [pk[0] for pk in primary_keys]

           

            # Drop existing table in MySQL
            mysql_cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")

            # Construct CREATE TABLE statement
            create_table_query = f"CREATE TABLE `{table_name}` ("
            columns = []
            for column in columns_info:
                
                col_name = column[0]
                col_type = column[1]
                col_length = column[2]
                col_precision = column[3]
                col_scale = column[4]
                is_nullable=column[5]


                # Map PostgreSQL type to MySQL type
                mysql_col_type = map_postgres_to_mysql(col_type)
                if 'varchar' in mysql_col_type.lower() and col_length:
                    mysql_col_type = f"VARCHAR({col_length})"
                elif 'decimal' in mysql_col_type.lower() and col_precision:
                    mysql_col_type += f"({col_precision},{col_scale})"
                elif mysql_col_type.lower() in ['timestamp', 'datetime']:
                    mysql_col_type += " DEFAULT CURRENT_TIMESTAMP"  # Set default to CURRENT_TIMESTAMP

               
               
                column_def = f"`{col_name}` {mysql_col_type}"
                # Handle NOT NULL constraint
                if is_nullable == 'NO':
                    column_def += " NOT NULL"
                columns.append(column_def)

            if primary_key_columns:
                create_table_query += ", ".join(columns)
                create_table_query += ", PRIMARY KEY (" + ", ".join([f"`{pk}`" for pk in primary_key_columns]) + ")"
            else:
                create_table_query += ", ".join(columns)

            create_table_query += ")"
            print(f"Executing SQL: {create_table_query}")

            try:
                mysql_cursor.execute(create_table_query)
            except Exception as e:
                print(f"Error creating table `{table_name}`: {e}")
                mysql_conn.rollback()
                continue

            print(f"Table `{table_name}` created successfully.")

            
        mysql_cursor.close()
        mysql_conn.close()
        postgres_cursor.close()
        postgres_conn.close()
     
    
    def migrate_all_tables(**kwargs):
        conf = kwargs.get('dag_run').conf if kwargs.get('dag_run') else {}
        load_type = conf.get('type', 'append') 
        

        postgres_hook = PostgresHook(postgres_conn_id='postgres_source')
        mysql_hook = MySqlHook(mysql_conn_id='mysql_dest')

        postgres_conn = postgres_hook.get_conn()
        postgres_cursor = postgres_conn.cursor()
        mysql_conn = mysql_hook.get_conn()
        mysql_cursor = mysql_conn.cursor()
        # Get all table names from PostgreSQL
        
        postgres_cursor.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
        """)
        tables = postgres_cursor.fetchall()

        for table_name_tuple in tables:
            table_name = table_name_tuple[0]


            print(f"Processing table: {table_name}")
            # Get data from PostgreSQL
            select_query=f"SELECT * FROM {table_name}"

            if (load_type == 'append'):
                if table_name  in ["actor_info","customer_list","film_list","nicer_but_slower_film_list"
                                ,"staff_list","payment", "payment_p2022_02","payment_p2022_03","payment_p2022_07", 
                                "payment_p2022_05","payment_p2022_06","payment_p2022_01","payment_p2022_04"] :
                    mysql_cursor.execute(f"SELECT MAX({get_id[table_name]}) FROM `{table_name}`")
                    max_id = mysql_cursor.fetchone()[0] 
                    if max_id:
                        select_query += f" WHERE {get_id[table_name]} > '{max_id}'"
                elif table_name in ["sales_by_film_category","sales_by_store"]:
                    mysql_cursor.execute(f"Delete from  `{table_name}`")
                else:
                    mysql_cursor.execute(f"SELECT MAX(last_update) FROM `{table_name}`")
                    last_update = mysql_cursor.fetchone()[0]
                    if last_update:
                        select_query += f" WHERE last_update > '{last_update}'"
                    



            postgres_cursor.execute(select_query)
            data = postgres_cursor.fetchall()
        
            # Process data
            data = [convert_lists_to_string(row) for row in data]
            column_names = [desc[0] for desc in postgres_cursor.description]

            # Construct INSERT statement
            insert_query = f"INSERT INTO `{table_name}` ({', '.join([f'`{col}`' for col in column_names])}) VALUES ({', '.join(['%s'] * len(column_names))})"
            try:
                mysql_cursor.executemany(insert_query, data)
                mysql_conn.commit()
            except Exception as e:
                print(f"Error inserting data into `{table_name}`: {e}")
                mysql_conn.rollback()
                mysql_cursor.close()
                mysql_conn.close()
                postgres_cursor.close()
                postgres_conn.close()
                return False
                
            print(f"Table `{table_name}` migrated successfully.")
       

        mysql_cursor.close()
        mysql_conn.close()
        postgres_cursor.close()
        postgres_conn.close()

    def decide_branch(**kwargs):
        conf = kwargs.get('dag_run').conf if kwargs.get('dag_run') else {}
        load_type = conf.get('type', 'append') 
        print(load_type)
        if load_type == 'overwrite': 
            return "run_create"
        else :
            return "run_dummy"
        
    

    def data_completeness_check(**kwargs):
        postgres_hook = PostgresHook(postgres_conn_id='postgres_source')
        mysql_hook = MySqlHook(mysql_conn_id='mysql_dest')
        pg_engine = postgres_hook.get_sqlalchemy_engine()
        mysql_engine = mysql_hook.get_sqlalchemy_engine()

      
        tables = pd.read_sql(f"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'", pg_engine)
        print(tables.head())
        tables=tables['table_name']
        print(tables.head())

        for table_name in tables:

            pg_count = pd.read_sql(f"SELECT COUNT(*) FROM {table_name}", pg_engine).iloc[0, 0]
            mysql_count = pd.read_sql(f"SELECT COUNT(*) FROM {table_name}", mysql_engine).iloc[0, 0]

            if pg_count != mysql_count:
                raise ValueError(f"Data completeness check failed for table {table_name}: {pg_count} records in Postgres, {mysql_count} records in MySQL")


    
    
   
    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=decide_branch,
        provide_context=True,
        dag=dag,
    )

    run_dummy = DummyOperator(
        task_id='run_dummy',
        dag=dag,
    )

    run_create = PythonOperator(
        task_id='run_create',
        python_callable=create_mysql_tables,
    )

    run_migrate = PythonOperator(
        task_id='run_migrate',
        python_callable=migrate_all_tables,
        trigger_rule=TriggerRule.ALL_DONE, 
    )


    complete = DummyOperator(
        task_id="complete",
        dag=dag,
        trigger_rule=TriggerRule.ALL_DONE,  
    )

    completeness_check = PythonOperator(
        task_id='completeness_check',
        python_callable=data_completeness_check,
        trigger_rule=TriggerRule.ALL_DONE,  

    )

    # Define the task flow
    branch_task >> [run_create, run_dummy]
    run_create >> run_migrate
    run_dummy >> run_migrate
    run_migrate >> complete
    complete >> completeness_check