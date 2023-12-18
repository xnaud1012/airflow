from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint
from flask_appbuilder import expose, BaseView as AppBuilderBaseView




bp = Blueprint(
               "review_plugin",
               __name__,
               template_folder="templates" # registers airflow/plugins/templates as a Jinja template folder
               )

class reviewAppBuilderBaseView(AppBuilderBaseView):
    default_view = "review"
    @expose("/", methods=['GET', 'POST'])
    def review(self):
        return self.render_template("env.html", content="DEV")



v_appbuilder_view = reviewAppBuilderBaseView()
v_appbuilder_package = {
    "name": "Review", # this is the name of the link displayed
    "category": "DEV", # 내가 생성하고자 하는 탭 이름.
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