from flask_appbuilder import BaseView, expose
from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint

ls =1
bp = Blueprint(
    "review_plugin",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static/review_plugin",
)
class ReviewView(BaseView):
    @expose('/')
    def index(self):
        # 리뷰 페이지의 HTML을 렌더링하는 로직을 추가합니다.
        return self.render_template("review.html")

class ReviewPlugin(AirflowPlugin):
    name = "review_plugin"
    appbuilder_views = [{
        "category": "Custom Menu",
        "name": "Review",
        "view": ReviewView()
    }]

