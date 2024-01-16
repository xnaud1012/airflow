from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint
import pandas as pd
from airflow.www.auth import has_access
from airflow.security import permissions
from flask_appbuilder import expose, BaseView as AppBuilderBaseView
from flask import jsonify
from flask import request, jsonify
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
import logging
import os
from datetime import datetime
import re
import cx_Oracle
from flask_wtf.csrf import generate_csrf

bp = Blueprint(
               "review_plugin",
               __name__,
               static_folder='static',
               template_folder="templates" ,
               static_url_path="/static/review_plugin"             
               )


class reviewAppBuilderBaseView(AppBuilderBaseView):
    default_view = "review"

    def extract_select_sql_query(self):

        current_directory = os.getcwd()
        print("현재 작업 디렉토리:", current_directory)
        sql_query = ''
        with open("../airflow/plugins/static/sql/select.sql", 'r') as f:    # 파일 읽기 및 처리

            for line in f:
                sql_query +=line;     
        cleaned_query = re.sub(r'[\t\s]+', ' ', sql_query)
        cleaned_query = re.sub(r'[\t\s]*,[\t\s]*', ', ', cleaned_query)
        cleaned_query = re.sub(r'[\t\s]*from[\t\s]*', ' FROM ', cleaned_query, flags=re.IGNORECASE)
        cleaned_query = re.sub(r'[\t\s]*where[\t\s]*', ' WHERE ', cleaned_query, flags=re.IGNORECASE)

        
        return cleaned_query;

    def extract_update_sql_query(self):

        current_directory = os.getcwd()
        sql_query = ''
        with open("../airflow/plugins/static/sql/update.sql", 'r') as f:    # 파일 읽기 및 처리

            for line in f:
                sql_query +=line;     
        cleaned_query = re.sub(r'[\t\s]+', ' ', sql_query)
        cleaned_query = re.sub(r'[\t\s]*,[\t\s]*', ', ', cleaned_query)
        cleaned_query = re.sub(r'[\t\s]*set[\t\s]*', ' SET ', cleaned_query, flags=re.IGNORECASE)
        cleaned_query = re.sub(r'[\t\s]*where[\t\s]*', ' WHERE ', cleaned_query, flags=re.IGNORECASE)

        
        return cleaned_query;
    
    @expose("/")
    @has_access(
        [
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
        ]
    )
    def review(self):
        csrf_token = generate_csrf()       
        return self.render_template("review.html", content="DEV", csrf_token=csrf_token)

    @expose('/getData', methods=['GET', 'POST'])
    def getData(self):
        # 데이터베이스 연결 가져오기
        import json
        
        with open('../airflow/plugins/static/sql/select2.sql', 'r') as file:
            sqlQuery = file.read()


        rdb = BaseHook.get_connection('oracle_xnaud')
        ora_con = cx_Oracle.connect(dsn=rdb.extra_dejson.get("dsn"),
                                    user=rdb.login,
                                    password=rdb.password,
                                    encoding="UTF-8")
        with ora_con :
            try:
                select_result_df = pd.read_sql(sqlQuery, ora_con)       
                json_result = select_result_df.to_json(orient='records')
                json_obj = json.loads(json_result)
                print(select_result_df)

                #컬럼 동적 생성을 위해 dataframe에서 column_info 추출 
                columns_info = [{"header": col, "name": col} for col in select_result_df.columns]
                return {"columns": columns_info, "data": json_obj}

            except Exception as e:
                logging.error("Error occurred: %s", e)
                print('Error occurred:', e)
                return "1"



    
    @expose('/updateData', methods=['POST'])
    def updateData(self):
        client_data = request.json;
        rowCount=0;

        try:
            client_data = request.json # ajax로부터 데이터 가져오기         
            pg_hook = PostgresHook('conn-db-postgres-custom')   # 데이터베이스 연결 가져오기          
            conn = pg_hook.get_conn()
            cursor = conn.cursor()
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            for item in client_data:
                item['dates'] = current_time
            query = self.extract_update_sql_query();  
            cursor.executemany(query, client_data);
            rowCount=cursor.rowcount
            conn.commit(); 
           
            return jsonify({"success": rowCount});
        
        except Exception as e:
            logging.error(f"Update failed: {e}")
            return jsonify({"error": str(e)}), 500
        
        finally:      
            cursor.close()   # 데이터베이스 연결과 커서는 사용 후에 반드시 닫아야 한다.
            conn.close()
          
        



v_appbuilder_view = reviewAppBuilderBaseView()
v_appbuilder_package = {
    "name": "review", # this is the name of the link displayed
    "category": "review", # 내가 생성하고자 하는 탭 이름.
    "view": v_appbuilder_view
}
   

class AirflowReviewPlugin(AirflowPlugin):
    name = "review_plugin"
    operators = []
    flask_blueprints = [bp]
    hooks = []
    executors = []
    admin_views = []
    appbuilder_views = [v_appbuilder_package]


