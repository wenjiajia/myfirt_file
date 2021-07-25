import json
import logging
import os
import pymysql

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S')

ENV_NAME_LIST_TMP = {'K8S_NEW_NAME_SPACE': 'qa',
                     'PANGU_YAML_DIR': '/opt/pangu/yaml',
                     'PANGU_DB_HOST': '',
                     'PANGU_DB_PORT': 3306,
                     'PANGU_DB_USER': "root",
                     'PANGU_DB_PASSWORD': '无密码此处置为空',
                     'PANGU_DB_NAME': 'pangu',
                     'DEBUG': "OFF"}

ENV_NAME_LIST = {}


class Mysql(object):
    def __init__(self):
        self._host = None
        self._port = None
        self._user = None
        self._password = None
        self._db = None
        self._connect = None
        self._cursor = None

    def set_host(self, host):
        self._host = host
        return self

    def set_port(self, port):
        self._port = int(port)
        return self

    def set_user(self, user):
        self._user = user
        return self

    def set_password(self, password):
        self._password = password
        return self

    def set_db(self, db):
        self._db = db
        return self

    def connect(self):
        try:
            self._connect = pymysql.connect(
                host=self._host,
                port=self._port,
                user=self._user,
                password=self._password,
                db=self._db
            )
            self._cursor = self._connect.cursor()
        except Exception as e:
            logging.error(e)
            logging.debug("mysql 连接信息: " + f"mysql -h {self._host} -P {self._port} -u {self._user} -p {self._password}")
            exit(1)

    def fetchone(self, sql):
        self._cursor.execute(sql)
        return list(self._cursor.fetchone())

    def get_insert_id(self, sql):
        self._cursor.execute(sql)
        insert_id = self._connect.insert_id()
        self._connect.commit()
        return insert_id


class PanguMysql(Mysql):
    def __init__(self, env, pangu_yaml_dir):
        super().__init__()
        self.sql_list = {
            "get_workload_list": "select * from workload",
            "get_workload_columns": "SHOW FULL COLUMNS FROM workload",
            "get_config_columns": "SHOW FULL COLUMNS FROM configmap",
            "get_config_data": "select * from configmap where name = '__CONFIG_NAME__'"
        }
        self.env = env
        self.pangu_yaml_dir = pangu_yaml_dir

    def get_workload_columns(self):
        self._cursor.execute(self.sql_list.get("get_workload_columns"))
        data = [_[0] for _ in [column for column in self._cursor.fetchall()]]
        data.pop(0)
        return data

    def get_config_map_columns(self):
        self._cursor.execute(self.sql_list.get("get_config_columns"))
        data = [_[0] for _ in [column for column in self._cursor.fetchall()]]
        data.pop(0)
        return data

    def insert_config_map(self):
        complete_config_list = []
        workload_list = self.get_workload_data()
        config_map_column_list = self.get_config_map_columns()
        workload_column_list = self.get_workload_columns()
        for workload in workload_list:
            workload = list(workload)
            workload.pop(0)
            for config in json.loads(workload[workload_column_list.index("config")]):
                conf_name = config.get("confname")
                if conf_name:
                    if conf_name in complete_config_list:
                        continue
                    else:
                        complete_config_list.append(conf_name)

                    data = self.get_config_insert_sql(conf_name)
                    source_name = data[config_map_column_list.index("name")]
                    new_name = source_name.replace("sg7-master", f"sg7-{self.env}")
                    source_content = data[config_map_column_list.index("content")]
                    new_content = source_content.replace('sg7-master', f'sg7-{self.env}')
                    copy_modify_file(source_content, new_content, self.env)
                    logging.info(f'{new_content} 配置文件已生成')
                    data[config_map_column_list.index("name")] = f'"{new_name}"'
                    data[config_map_column_list.index("content")] = f'"{new_content}"'
                    data[config_map_column_list.index("envtype")] = f'"{self.env}"'
                    data[config_map_column_list.index("des")] = f'"{data[config_map_column_list.index("des")]}"'
                self.get_insert_id(
                    f'INSERT INTO `{self._db}`.`configmap` ({",".join([str(_) for _ in config_map_column_list])}) VALUE ({",".join([str(_) for _ in data])})')

    def get_config_insert_sql(self, config_name):
        data = self.fetchone(self.sql_list.get('get_config_data').replace("__CONFIG_NAME__", config_name))
        data.pop(0)
        return data

    def insert_workload(self):
        column_list = self.get_workload_columns()
        data_list = self.get_workload_data()
        for data in data_list:
            data = list(data)
            # remove id
            source_id = data.pop(0)
            source_name = data[column_list.index("name")]
            source_conf = data[column_list.index("config")]
            data[column_list.index("replica")] = 1
            data[column_list.index("minpod")] = 1
            data[column_list.index("maxpod")] = 1
            data[column_list.index("name")] = f"\"{source_name}\""
            data[column_list.index("imagename")] = f"\"{data[column_list.index('imagename')]}\""
            data[column_list.index("envtype")] = f'"{self.env}"'
            data[column_list.index("config")] = json.dumps(source_conf.replace("sg7-master", f"sg7-{self.env}"))
            data[column_list.index("nfs")] = json.dumps(data[column_list.index("nfs")])
            data[column_list.index("resource")] = json.dumps(data[column_list.index("resource")])

            new_id = self.get_insert_id(
                f'INSERT INTO `{self._db}`.`workload` ({",".join([str(_) for _ in column_list])}) VALUE ({",".join([str(_) for _ in data])})'.replace(
                    ",,", ",'',"))
            logging.info(f"{data[column_list.index('envtype')]}环境已新增{source_name}")
            copy_modify_file(f"{self.pangu_yaml_dir}/{source_id}.yaml", f"{self.pangu_yaml_dir}/{new_id}.yaml",
                             self.env)

    def get_workload_data(self):
        self._cursor.execute(self.sql_list.get("get_workload_list"))
        return self._cursor.fetchall()


# def insert_data(insert_sql_list, pangu_mysql_object: PanguMysql):
#     mysql = pangu_mysql_object
#     for _ in insert_sql_list:

def copy_modify_file(file_from, file_to, env):
    with open(file_from) as f:
        data = f.read()
        data = data.replace('namespace: master', f"namespace: {env}")
        data = data.replace('name: sg7-master', f"name: sg7-{env}")

    with open(file_to, 'w') as f:
        f.write(data)

    logging.debug(f"{file_to} 已生成")


def init_db():
    mysql = PanguMysql(ENV_NAME_LIST.get('K8S_NEW_NAME_SPACE'),
                       ENV_NAME_LIST.get('PANGU_YAML_DIR'))
    mysql.set_host(ENV_NAME_LIST.get('PANGU_DB_HOST')) \
        .set_port(ENV_NAME_LIST.get('PANGU_DB_PORT')) \
        .set_user(ENV_NAME_LIST.get('PANGU_DB_USER')) \
        .set_password(ENV_NAME_LIST.get('PANGU_DB_PASSWORD')) \
        .set_db(ENV_NAME_LIST.get('PANGU_DB_NAME')) \
        .connect()
    logging.info("MYSQL 连接成功")
    return mysql


def main():
    pre_fly()
    set_log_level()

    mysql = init_db()
    # mysql.get_workload()
    mysql.insert_config_map()

    mysql.insert_workload()


def set_log_level():
    if ENV_NAME_LIST.get("DEBUG") == "ON":
        level = logging.DEBUG
        for _ in logging.getLogger().handlers:
            _.setLevel(level)


def pre_fly():
    for _ in ENV_NAME_LIST_TMP.keys():
        if not os.environ.get(_):
            usage()
            exit(1)
        else:
            ENV_NAME_LIST[_] = os.environ.get(_)


def usage():
    print("补充完空白行，执行以下shell后重新执行本脚本")
    for _, j in zip(ENV_NAME_LIST_TMP.keys(), ENV_NAME_LIST_TMP.values()):
        if j:
            print(f"# 默认为{j}")
        else:
            print("# 待补充")
        print(f'export {_}={j}')


if __name__ == '__main__':
    main()