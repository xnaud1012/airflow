import pendulum
from airflow import DAG
from airflow.providers.jdbc.hooks.jdbc import JdbcHook
from airflow.decorators import task
import jpype
import jpype.imports
from jpype.types import *
with DAG(
    dag_id='jdbc_bridge_mssql',
    start_date=pendulum.datetime(2024, 1, 1, tz='Asia/Seoul'),
    #schedule="*/2 * * * *",
    schedule='0 0 * * *',
    catchup=False
) as dag:
    

    class LoggingJdbcHook(JdbcHook):
        def get_conn(self):
            # Airflow Connection 객체에서 connection 정보를 가져옵니다.
            conn = self.get_connection(getattr(self, self.conn_name_attr))
            conn_config = conn.extra_dejson
            
            # 'Extra' 필드에서 드라이버 클래스 이름(jclassname)을 추출합니다.
            jclassname = conn_config.get('driver', None)
            
            # jclassname 값을 로깅합니다.
            logging.info(f"JdbcHook.get_conn() - Driver class name: {jclassname}*********************************************************!!!!!!!!!!!!!!!!!!!!!!")

            # JDBC Hook의 원래 get_conn() 메서드를 호출하여 Connection 객체를 반환합니다.
            return super().get_conn()
    

    @task(task_id='execute')
    def execute(**kwargs):
        print('start')
        if not jpype.isJVMStarted():
            jpype.startJVM()

        java_class_path = jpype.java.lang.System.getProperty("java.class.path")
        print(f"Java Class Path: {java_class_path}")

        Driver = jpype.JClass('com.microsoft.sqlserver.jdbc.SQLServerDriver')
        jpype.java.sql.DriverManager.registerDriver(Driver())

        jdbc_hook = JdbcHook(jdbc_conn_id="MSSQL_JDBC_CONN")


        print(jdbc_hook)
        print('************************************************************************************************************')
        url = 'jdbc:sqlserver://10.91.xx.xx:xxxx;databaseName=xnaud;user=sa;password=aaaaa;trustServerCertificate=true;'
        conn = jpype.java.sql.DriverManager.getConnection(url)
        stmt = conn.createStatement()


        #jdbc_hook = LoggingJdbcHook(jdbc_conn_id="MSSQL_JDBC_CONN")
        resultSet = stmt.executeQuery("SELECT TOP 5 * FROM DEATH")
        while resultSet.next():
          print(resultSet.getString(1))  # 첫 번째 컬럼의 데이터 출력

        resultSet.close()
        stmt.close()
        conn.close()

        if jpype.isJVMStarted():
            jpype.shutdownJVM()

    execute()


