'''''
this script used configparser to parse the requred parameter which help to make a connectione
to a mongo database. Here conn_str is the function which returns the host for mongoengine connect method
'''''
import configparser
import urllib
def conn_str():
    confg=configparser.ConfigParser()
    confg.read_file(open('dbs_config.conf'))

    #extract the connnecion paramters from config file "dbs_config"
    m_pass=confg.get('dbs_conf','mongo_pass')
    m_user=confg.get('dbs_conf','mongo_user')
    m_port=confg.get('dbs_conf','mongo_port')
    m_host=confg.get('dbs_conf','db_host')
    m_db=confg.get('dbs_conf','mdb_name')

    host_1=f"mongodb://{m_user}:"+ urllib.parse.quote(m_pass) + f"@{m_host}:{m_port}/{m_db}?authSource=admin"
    return host_1



