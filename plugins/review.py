from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint
from flask_appbuilder import expose, BaseView as AppBuilderBaseView
import psycopg2


bp = Blueprint(
               "review_plugin",
               __name__,
               static_folder='static',
               template_folder="templates", # registers airflow/plugins/templates as a Jinja template folder
               static_url_path="/static/review_plugin"
               )

class reviewAppBuilderBaseView(AppBuilderBaseView):
    default_view = "review"
    @expose("/", methods=['GET', 'POST'])
    def review(self):
       
        return self.render_template("review_plugin/env.html", content="DEV")



v_appbuilder_view = reviewAppBuilderBaseView()
v_appbuilder_package = {
    "name": "Review", # this is the name of the link displayed
    "category": "Custom", # 내가 생성하고자 하는 탭 이름.
    "view": v_appbuilder_view
}

class AirflowTestPlugin(AirflowPlugin):
    name = "review_plugin"
    operators = []
    flask_blueprints = [bp]
    hooks = []
    executors = []
    admin_views = []
    appbuilder_views = [v_appbuilder_package]
