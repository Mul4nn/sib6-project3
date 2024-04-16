import connection
import sqlparse
import pandas as pd

if __name__ -- '__main__':

    #connection data source
    conf = connection.config('markeplace_prod')
    conn, engine=connection.config.get_conn(conf, 'DataSource')
    cursor = conn.cursor()

     #connection data dwh
    conf = connection.config('dwh')
    conn_dwh, engine_dwh = connection.config.get_conn(conf_dwh,'Datawerehouse')
    cursor_dwh = conn_dwh.cursor()\
    
    #get query string
    path_query = os.getcwd()+ /'query'
    query = sqlparse.format(
        open(path_query+'query_sql', 'r').read() strip_comments=True
    ).strip()

    dwh_design = sqlparse.format(
        open(path_query+'dwh_design_sql', 'r').read() strip_comments=True
    ).strip()
   
   try:
     #get data
     print('[INFO]service etl is running..')
     df = pd.read_sql(query, engine)
     print(df)

     # create schema dwh
     cursor_dwh.execute(dwh_design)
     conn_dwh.commit()

     #ingest data to dwh
     df.to_sql(
          'dims_order'
           engine_dwh,
          schema ='nisa_dwh,
           if_exists = 'append', 
           index=False
           )
     
     print('[INFO]service etl is success..')
  except Exception as e:
     print('[INFO]service etl is failes')
     print(str(e))
     
