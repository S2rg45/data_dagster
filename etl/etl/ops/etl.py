
from ..db_con import  connection
from dagster import op, Output, Out

import pandas as pd


@op(out={"df": Out(), "name_table": Out()})
def get_data(context):
  try:
    read_excel = '../../../Matriz_de_adyacencia_data_team.xlsx'
    xl = pd.ExcelFile(read_excel)
    context.log.info(xl)
    sheets = xl.sheet_names
    for item in sheets:
      context.log.info(item)
      dataframe = pd.read_excel(read_excel, sheet_name= item)
      number_columns = len(dataframe.columns)
      if number_columns > 5:
        context.log.info("OK", xl)
        
        data_range = [item for item in range(number_columns)]
        data_columns = dataframe.iloc[:,data_range[2:]]
        data_drop_nan = data_columns.dropna()
        data_all = data_drop_nan.drop(index=0)
        name_table = "stg_habi_data"
        yield Output(data_all, "df")
        yield Output(name_table, "name_table")
        # load_and_create_table_data = load_data(data_all, name_table)
      else:
        data_range = [item for item in range(number_columns)]
        data_columns = dataframe.iloc[:,data_range]
        data_columns.columns =['id', 'ids', 'name']
        data_ = data_columns.dropna()
        name_table = "stg_habi_user"
        yield Output(data_, "df")
        yield Output(name_table, "name_table")
        # load_and_create_table_user = load_data(data_, name_table)
        
  except Exception as e:
    print("No puede extrar datos {}".format(e))
    
        
    
@op
def load_data(context, data, name_table):
  engine = connection()
  data.to_sql(f'{name_table}', engine[1], if_exists='replace', index=False, schema="public" )
  connection_postgres = connection()      
  connection_postgres[0].autocommit = True
  cursor = connection_postgres[0].cursor()
  sql1 = 'select * from {};'.format(name_table)
  cursor.execute(sql1)
  for i in cursor.fetchall():
      print(i)
  # connection_postgres[0].commit()
  connection_postgres[0].close()
      
      
    

    


