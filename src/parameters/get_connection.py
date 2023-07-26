#parse the parameter to connect to mongo database
import configparser
import urllib.parse
confg = configparser.ConfigParser()
confg.read_file(open('../conf/cong.conf'))
def get_mongo_conn_str():
    

    # check the connection to the database
    m_pass = confg.get('db_conf', 'mongo_pass')
    m_user = confg.get('db_conf', 'mongo_user')
    m_port = confg.get('db_conf', 'mongo_port')
    m_host = confg.get('db_conf', 'mongo_db_host')

    m_pass_encoded = urllib.parse.quote(m_pass)

    host_1 = f"mongodb://{m_user}:{m_pass_encoded}@{m_host}:{m_port}/waterpoints?authSource=admin"

    return host_1


def get_postres_conn_str():
    # check the connection to the database
    p_pass = confg.get('db_conf', 'postgres_pass')
    p_user = confg.get('db_conf', 'postgres_user')
    p_port = confg.get('db_conf', 'postgres_port')
    p_host = confg.get('db_conf', 'postgres_db_host')

    p_pass_encoded = urllib.parse.quote(p_pass)

    host_2 = f"user={p_user} password={p_pass_encoded} dbname=s3cr3t host={p_host} port={p_port}"
    
    return host_2