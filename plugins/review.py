from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint
from airflow.www.auth import has_access
from airflow.security import permissions
from flask_appbuilder import expose, BaseView as AppBuilderBaseView
from flask import jsonify
from airflow.providers.postgres.hooks.postgres import PostgresHook
import psycopg2
import os
import json
import re

bp = Blueprint(
               "review_plugin",
               __name__,
               static_folder='static',
               template_folder="templates" ,
               static_url_path="/static/review_plugin"             
               )


class reviewAppBuilderBaseView(AppBuilderBaseView):
    default_view = "review"

    def extract_sql_query(self):

        current_directory = os.getcwd()
        print("현재 작업 디렉토리:", current_directory)
        sql_query = ''
        with open("../airflow/plugins/static/sql/psql.sql", 'r') as f:    # 파일 읽기 및 처리

            for line in f:
                sql_query +=line;     
        cleaned_query = re.sub(r'[\t\s]+', ' ', sql_query)
        cleaned_query = re.sub(r'[\t\s]*,[\t\s]*', ', ', cleaned_query)
        cleaned_query = re.sub(r'[\t\s]*from[\t\s]*', ' FROM ', cleaned_query, flags=re.IGNORECASE)

        
        return cleaned_query;
    
    @expose("/")
    @has_access(
        [
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
        ]
    )
    def review(self):
       
       
        return self.render_template("env.html", content="DEV")
    
    
    @expose('/getData', methods=['GET', 'POST'])
    def getData(self):
        print('come in!!!!!')
        pg_hook = PostgresHook('conn-db-postgres-custom') 
        # 데이터베이스 연결 가져오기
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        query = self.extract_sql_query();
        cursor.execute(query);
        data = cursor.fetchall();

        column_names = [desc[0] for desc in cursor.description]
        result = []

        for row in data:
            row_data = {}
            for i, col_name in enumerate(column_names):
                row_data[col_name] = row[i]
            result.append(row_data)              
        new_result= {"result_data":result}
  
        return new_result




v_appbuilder_view = reviewAppBuilderBaseView()
v_appbuilder_package = {
    "name": "review", # this is the name of the link displayed
    "category": "custom", # 내가 생성하고자 하는 탭 이름.
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


