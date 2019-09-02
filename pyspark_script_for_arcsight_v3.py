import pyspark
import time
import subprocess
import json
import re
import copy
import traceback
import socket
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, FloatType, TimestampType, DecimalType
from datetime import datetime
from decimal import Decimal
import sys

def progressBar(name, value, endvalue, bar_length = 50, width = 20):
        percent = float(value) / endvalue
        arrow = '-' * int(round(percent*bar_length) - 1) + '>'
        spaces = ' ' * (bar_length - len(arrow))
        sys.stdout.write("\r{0: <{1}} : [{2}]{3}%".format(\
                         name, width, arrow + spaces, int(round(percent*100))))
        sys.stdout.flush()
        if value == endvalue:        
             sys.stdout.write('\n\n')

def change_dir_permissions(result_dir):
    cmd0 = ['whoami']
    cmd1 = ['sudo', '-u', 'hdfs', 'hadoop', 'fs', '-chown', 'root', '/tmp/spark']
    cmd2 = ['sudo', '-u', 'hdfs', 'hadoop', 'fs', '-chmod', '777', '/tmp/spark']
    cmd3 = ['sudo', '-u', 'hdfs', 'hadoop', 'fs', '-chmod', '-R', '777', '/tmp/spark']

    lst = [cmd0, cmd1, cmd2, cmd3]
    for i in lst:
        subprocess.call(i)

def dir_list(dir1, hdfs_url, sc, count_of_dirs):
    URI = sc._gateway.jvm.java.net.URI
    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    fs = FileSystem.get(URI(hdfs_url), sc._jsc.hadoopConfiguration())
    t =fs.listStatus(Path(dir1))
    result = []
    result_new = []
    dir_lst = []
    for fileStatus in t:
        result.append(str(fileStatus.getPath()))
    for i in result:
        t = fs.listStatus(Path(i[len(hdfs_url):]))
        for j in t:
            result_new.append(str(j.getPath()))
    if len(result_new):
        if len(result_new)<count_of_dirs: 
            dir_lst = result_new
        else:
            dir_lst = result_new[:count_of_dirs]
        dir_lst_tmp = []
        for i in dir_lst:
            if not '.tmp' in i:
                dir_lst_tmp.append(i)
        return dir_lst_tmp
            

def delete_temp(dir, sc, hdfs_url):
    URI = sc._gateway.jvm.java.net.URI
    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    fs = FileSystem.get(URI(hdfs_url), sc._jsc.hadoopConfiguration())
    fs.delete(Path(dir[len(hdfs_url):]), True)
    print('Delete done')
    


def hdfs_save(lst, schema, sqlContext, hdfs_url, result_dir, columns_to_drop):
    date = datetime.today().strftime('%Y-%m-%d')
    if len(lst):
        new_sum_lst = []
        for i in lst:
            if i:
                new_sum_lst.append(i)
        try:
            df = sqlContext.createDataFrame(new_sum_lst,schema=schema)
        except:
            print('Can`t create dataframe')
            print(traceback.format_exc())
        else:
            if len(columns_to_drop) != 0:
                df = df.drop(*columns_to_drop)
            df.write.mode('append').format('parquet').save('{0}{1}/{2}'.format(hdfs_url, 
                                                                    result_dir, 
                                                                    date))
            print('Good save')

def make_rdd(dir, sc):
    d1 = sc.textFile(dir)
    d1 = d1.filter(lambda x: x is not None)
    d1 = d1.filter(lambda x: x != "")
    d1 = d1.filter(lambda x: len(x) != 0)
    print('RDD make: true')
    return d1

def rdd_to_list_of_dict(rdd, parsing_function):
    if rdd:
        sum_lst = []
        rdd_col = rdd.collect()
        rdd_col_len = len(rdd_col)
        print('Len of RDD: {}'.format(str(rdd_col_len)))
        j = 1
        for x in rdd_col:
            progressBar('Parsing RDD', j, rdd_col_len)
            #print('Parsing now {} of {}'.format(str(j), str(rdd_col_len)))
            #print(x)
            dic_new = {}
            x = x.replace('\\', '/')
            x = x.replace('@', '')
            dic_new = parsing_function(x)
            if dic_new:
                sum_lst.append(dic_new)
            j = j + 1
        print('List of dicts make: true')
        return sum_lst

def main_def_for_spark(temp_dir, result_dir, hdfs_url, sc, sqlContext, schema, parsing_function, columns_to_drop=None, count_of_dirs=None):
    if columns_to_drop == None:
        columns_to_drop = []
    if count_of_dirs == None:
        count_of_dirs = 50
    dirs = dir_list(temp_dir, hdfs_url, sc, count_of_dirs)
    if dirs:
        print('Files is: ')
        print(dirs)
        for i in dirs:
            print('<===================={}====================>'.format(datetime.now().isoformat()))
            print('Current file: ')
            print(i)
            rdd = make_rdd(i, sc)
            lst = rdd_to_list_of_dict(rdd, parsing_function)
            hdfs_save(lst, schema, sqlContext, hdfs_url, result_dir, columns_to_drop)
            print('Saved parquet from {}'.format(i))
            delete_temp(i, sc, hdfs_url)
            print('Deleted dir: {}'.format(i))
    else:
        print('Dirs are empty!!!!!')
    change_dir_permissions(result_dir)




    
def parsing_function_ArcSigth(string):
    #Default list of fields
    default_lst = [
        '_cef_ver', 'access', 'act', 'ad_additional_info',
        'ad_additional_info2', 'ad_event_record_id', 'ad_key_length', 'ad_lm_package_name',
        'ad_logon_guid', 'ad_message', 'ad_object_server', 'ad_opcode',
        'ad_operation_type', 'ad_process_id', 'ad_properties', 'ad_raw_event',
        'ad_service_sid', 'ad_subject_logon_id', 'ad_subject_user_name', 'ad_subject_user_sid',
        'ad_target_info', 'ad_target_sid', 'ad_target_user_sid', 'ad_thread_id',
        'ad_ticket_encryption_type', 'ad_ticket_options', 'ad_transmitted_services', 'ad_version',
        'ad_workstation_name', 'agent_address', 'agent_host_name', 'agent_name',
        'agent_type', 'agent_zone_uri', 'aggregated_event_count', 'agt',
        'ahost', 'aid', 'amac', 'app', 'cs1label', 'cs2label', 'cs3label', 'cs4label', 'cs5label',
        'art', 'at', 'atz', 'authentication_package_name',
        'av', 'base_event_ids', 'in', 'out', 'cs1', 'cs2', 'cs3', 'cs4', 'cs5',
        'cat', 'catdt', 'category_behavior', 'category_device_group',
        'category_object', 'category_outcome', 'category_significance', 'cef_version',
        'client_address', 'cnt', 'configuration_resource', 'count_str_trunc', 'cn1', 'cn2', 'cn3',
        'destination_asset_name', 'cn1lable', 'cn2lable', 'cn3lable',
        'destination_dns_domain', 'destination_id', 'destination_mac_address',
        'dpt', 'destination_process_name', 'destination_service_name',
        'destination_zone_name', 'device_custom_date1', 'device_custom_date2',
        'destination_zone_uri', 'device_asset_name',
        'device_count', 'device_custom_date', 'device_custom_number', 'device_custom_string',
        'device_direction', 'device_dns_domain', 'device_domain', 'device_event_class_id',
        'device_external_id', 'device_facility', 'device_inbound_interface',
        'device_name', 'device_nt_domain', 'device_outbound_interface',
        'device_process_name', 'device_product', 'device_severity',
        'device_vendor', 'device_version', 'device_zone_name', 'device_zone_uri',
        'dhost', 'dntdom', 'dpriv', 'dproc',
        'dst', 'dtz', 'duid', 'duser', 'device_custom_date1lable', 'device_custom_date2lable',
        'dvc', 'dvchost', 'dvcmac', 'end',
        'event_count__slc_', 'event_id', 'event_source', 'event_throughput',
        'event_throughput__slc_', 'eventlog_category', 'external_id', 'file_create_time',
        'file_id', 'file_modification_time', 'file_path',
        'file_permission', 'fsize', 'file_type', 'flex_date1',
        'flex_number1', 'flex_number2', 'flex_string1', 'flex_string2', 'fname', 'geid',
        'generator', 'generator_id', 'impersonation_level', 'last_time',
        'logon_guid', 'logon_type', 'message', 'mrt',
        'msg', 'name', 'new_time', 'object_type',
        'old_file_create_time', 'old_file_hash', 'old_file_modification_time', 'old_file_name',
        'old_file_path', 'old_file_permission', 'old_file_size', 'old_file_type',
        'orrelated_event_count', 'package_name', 'pre_authentication_type', 'previous_time',
        'process_id', 'raw_event', 'raw_event_character_throughput', 'raw_event_character_throughput__slc_',
        'raw_event_length__slc_', 'reason', 'request_context', 'request_cookies',
        'request_method', 'request_protocol', 'request', 'request_url_authority',
        'request_url_file_name', 'request_url_host', 'request_url_port', 'request_url_query',
        'result_code', 'rrequest_client_application', 'rt', 'shost',
        'source_asset_name', 'source_dns_domain',
        'source_i_pv6_address', 'smac', 'sntdom', 
        'sproc', 'source_service_name', 'source_user_name',
        'spriv', 'source_zone_name', 'source_zone_uri', 'spt',
        'src', 'start', 'status', 'suser', 'parition_year', 'parition_month', 'parition_day',
        'total_event_count', 'total_raw_event_length', 'proto', 'type',
        'vulnerability', 'vulnerability_id', 'vulnerability_name', 'vulnerability_resource',
        'agent_dns_domain', 'agent_nt_domain', 'agent_translated_address', 'agent_translated_zone_external_id',
        'agent_translated_zone_uri', 'agent_zone_external_id', 'attacker_geo_country_code', 'attacker_geo_location_info',
        'attacker_geo_postal_code', 'attacker_geo_region_code', 'attacker_translated_zone_external_id', 'attacker_translated_zone_uri',
        'c6a1', 'c6a1label', 'c6a2', 'c6a2label',
        'c6a3', 'c6a3label', 'c6a4', 'c6a4label',
        'category_technique', 'category_tuple_description', 'cn1label', 'cn2label',
        'cn3label', 'crypto_signature', 'cs6', 'cs6label',
        'customer_external_id', 'customer_uri', 'destination_geo_country_code', 'destination_geo_location_info',
        'destination_geo_postal_code', 'destination_geo_region_code', 'destination_translated_address', 'destination_translated_port',
        'destination_translated_zone_external_id', 'destination_translated_zone_uri', 'destination_zone_external_id', 'device_custom_date1label',
        'device_custom_date2label', 'device_payload_id', 'device_process_name_', 'device_translated_address',
        'device_translated_zone_external_id', 'device_translated_zone_uri', 'device_zone_external_id', 'dlat',
        'dlong', 'dmac', 'dpid', 'dvcpid',
        'file_hash', 'flex_date1label', 'flex_number1label', 'flex_number2label',
        'flex_string1label', 'flex_string2label', 'generator_external_id', 'generator_uri',
        'old_file_id', 'outcome', 'request_client_application', 'slat',
        'slong', 'source_geo_country_code', 'source_geo_location_info', 'source_geo_postal_code',
        'source_geo_region_code', 'source_translated_address', 'source_translated_port', 'source_translated_zone_external_id',
        'source_translated_zone_uri', 'source_zone_external_id', 'spid', 'suid',
        'target_geo_country_code', 'target_geo_location_info', 'target_geo_postal_code', 'target_geo_region_code',
        'target_translated_zone_uri', 'vulnerability_external_id', 'vulnerability_uri']
    # Create the empty dicts
    values = dict()
    # This regex separates the string into the CEF header and the extension
    # data.  Once we do this, it's easier to use other regexes to parse each
    # part.
    header_re = r'((CEF:\d+)([^=\\]+\|){,7})(.*)'
    res = re.search(header_re, string)
    if res:
        header = res.group(1)
        extension = res.group(4)
        # Split the header on the "|" char.  Uses a negative lookbehind
        # assertion to ensure we don't accidentally split on escaped chars,
        # though.
        spl = re.split(r'(?<!\\)\|', header)
        # Since these values are set by their position in the header, it's
        # easy to know which is which.
        values["DeviceVendor"] = spl[1]
        values["DeviceProduct"] = spl[2]
        values["DeviceVersion"] = spl[3]
        values["DeviceEventClassID"] = spl[4]
        values["DeviceName"] = spl[5]
        if len(spl) > 6:
            values["DeviceSeverity"] = spl[6]
        # The first value is actually the CEF version, formatted like
        # "CEF:#".  Ignore anything before that (like a date from a syslog message).
        # We then split on the colon and use the second value as the
        # version number.
        cef_start = spl[0].find('CEF')
        if cef_start == -1:
            return None
        (cef, version) = spl[0][cef_start:].split(':')
        values["CEFVersion"] = version
        # The ugly, gnarly regex here finds a single key=value pair,
        # taking into account multiple whitespaces, escaped '=' and '|'
        # chars.  It returns an iterator of tuples.
        spl = re.findall(r'([^=\s]+)=((?:[\\]=|[^=])+)(?:\s|$)', extension)
        for i in spl:
            # Split the tuples and put them into the dictionary
            values[i[0]] = i[1]
        # Process custom field labels
        for key in list(values.keys()):
            # If the key string ends with Label, replace it in the appropriate
            # custom field
            if key[-5:] == "Label":
                customlabel = key[:-5]
                # Find the corresponding customfield and replace with the label
                for customfield in list(values.keys()):
                    if customfield == customlabel:
                        values[values[key]] = values[customfield]
                        del values[customfield]
                        del values[key]
        #
        temp_dic = {}
        for i in values:
            j = re.sub(r"([a-z](?=[A-Z])|[A-Z](?=[A-Z][a-z]))", r"\1 ", i)
            temp_dic[j.lower()] = values[i]
        values = copy.deepcopy(temp_dic)
        # Replace the fucking characters from keys 
        sub_pattern = r'[ -.,;{}()(\n)\\(\t=)]'
        temp_dic = {}
        for i in values.keys():
            j = re.sub(sub_pattern, '_', i)
            temp_dic[j] = values[i]
        values = copy.deepcopy(temp_dic)
        # Delete elements if field not in default list
        for i in values.keys():
            if i not in default_lst:
                del values[i]
        # Add missing element from default list to dict
        for i in default_lst:
            if i not in values.keys():
                values[i] = None


        # Transform some values to correct types
        list_to_ts = ['old_file_modification_time', 'old_file_create_time', 'file_modification_time', 'end', 'rt', 'art', 'start', 'device_custom_date1', 'device_custom_date2', 'flex_date1', 'file_create_time']
        list_to_decimal = ['event_id', 'ad_event_record_id', 'ad_process_id', 'cn1', 'cn2', 'cn3', 'dlat', 'dlong', 'slat', 'slong']
        list_to_int = ['old_file_size', 'spt', 'in', 'out', 'dpt', 'device_direction', 'cnt', 'flex_number1', 'flex_number2', 'fsize', 'parition_year', 'parition_month', 'parition_day','destination_translated_port', 'dpid', 'dvcpid', 'source_translated_port', 'spid']
        
        for list_element in list_to_ts:
            if values[list_element]:
                values[list_element] = datetime.fromtimestamp(float('{}.{}'.format(values[list_element][0:-3], values[list_element][-3:])))
        
        values['parition_year'] = values['rt'].year
        values['parition_month'] = values['rt'].month
        values['parition_day'] = values['rt'].day

        for list_element in list_to_decimal:
            if values[list_element]:
                values[list_element] = Decimal(values[list_element])
        for list_element in list_to_int:
            if values[list_element]:
                values[list_element] = int(values[list_element])

       
        
        


        # Return result
        return values
    else:
        return None


if __name__ == "__main__":
    if not 'sc' in globals():
        sc = pyspark.SparkContext()
    sc.setLogLevel('WARN')
    sqlContext = SQLContext(sc)
    temp_dir = '/tmp/flume_tmp/ArcSight'
    hdfs_url = 'hdfs://cloudera.main.lan'
    result_dir = '/tmp/spark/ArcSight/'
    schema = StructType([
        StructField(name='_cef_ver', dataType=StringType(), nullable=True),
        StructField('access', StringType(), True),
        StructField('act', StringType(), True),
        StructField('ad_additional_info', StringType(), True),
        StructField('ad_additional_info2', StringType(), True),
        StructField('ad_event_record_id', DecimalType(), True),
        StructField('ad_key_length', StringType(), True),
        StructField('ad_lm_package_name', StringType(), True),
        StructField('ad_logon_guid', StringType(), True),
        StructField('ad_message', StringType(), True),
        StructField('ad_object_server', StringType(), True),
        StructField('ad_opcode', StringType(), True),
        StructField('ad_operation_type', StringType(), True),
        StructField('ad_process_id', StringType(), True),
        StructField('ad_properties', StringType(), True),
        StructField('ad_raw_event', StringType(), True),
        StructField('ad_service_sid', StringType(), True),
        StructField('ad_subject_logon_id', StringType(), True),
        StructField('ad_subject_user_name', StringType(), True),
        StructField('ad_subject_user_sid', StringType(), True),
        StructField('ad_target_info', StringType(), True),
        StructField('ad_target_sid', StringType(), True),
        StructField('ad_target_user_sid', StringType(), True),
        StructField('ad_thread_id', StringType(), True),
        StructField('ad_ticket_encryption_type', StringType(), True),
        StructField('ad_ticket_options', StringType(), True),
        StructField('ad_transmitted_services', StringType(), True),
        StructField('ad_version', StringType(), True),
        StructField('ad_workstation_name', StringType(), True),
        StructField('agent_address', StringType(), True),
        StructField('agent_host_name', StringType(), True),
        StructField('agent_name', StringType(), True),
        StructField('agent_type', StringType(), True),
        StructField('agent_zone_uri', StringType(), True),
        StructField('aggregated_event_count', StringType(), True),
        StructField('agt', StringType(), True),
        StructField('ahost', StringType(), True),
        StructField('aid', StringType(), True),
        StructField('amac', StringType(), True),
        StructField('app', StringType(), True),
        StructField('art', TimestampType(), True),
        StructField('at', StringType(), True),
        StructField('atz', StringType(), True),
        StructField('authentication_package_name', StringType(), True),
        StructField('av', StringType(), True),
        StructField('base_event_ids', StringType(), True),
        StructField('in', IntegerType(), True),
        StructField('out', IntegerType(), True),
        StructField('cat', StringType(), True),
        StructField('catdt', StringType(), True),
        StructField('category_behavior', StringType(), True),
        StructField('category_device_group', StringType(), True),
        StructField('category_object', StringType(), True),
        StructField('category_outcome', StringType(), True),
        StructField('category_significance', StringType(), True),
        StructField('cef_version', StringType(), True),
        StructField('client_address', StringType(), True),
        StructField('cn1', DecimalType(), True),
        StructField('cn2', DecimalType(), True),
        StructField('cn3', DecimalType(), True),
        StructField('cn1lable', StringType(), True),
        StructField('cn2lable', StringType(), True),
        StructField('cn3lable', StringType(), True),
        StructField('cnt', IntegerType(), True),
        StructField('configuration_resource', StringType(), True),
        StructField('count_str_trunc', StringType(), True),
        StructField('cs1', StringType(), True),
        StructField('cs2', StringType(), True),
        StructField('cs3', StringType(), True),
        StructField('cs4', StringType(), True),
        StructField('cs1label', StringType(), True),
        StructField('cs2label', StringType(), True),
        StructField('cs3label', StringType(), True),
        StructField('cs4label', StringType(), True),
        StructField('cs5label', StringType(), True),
        StructField('cs5', StringType(), True),
        StructField('destination_asset_name', StringType(), True),
        StructField('destination_dns_domain', StringType(), True),
        StructField('destination_id', StringType(), True),
        StructField('destination_mac_address', StringType(), True),
        StructField('dpt', IntegerType(), True),
        StructField('destination_process_name', StringType(), True),
        StructField('destination_service_name', StringType(), True),
        StructField('destination_user_name', StringType(), True),
        StructField('destination_zone_uri', StringType(), True),
        StructField('device_asset_name', StringType(), True),
        StructField('device_count', StringType(), True),
        StructField('device_custom_date1', TimestampType(), True),
        StructField('device_custom_date2', TimestampType(), True),
        StructField('device_custom_date1lable', StringType(), True),
        StructField('device_custom_date2lable', StringType(), True),
        StructField('device_custom_number', StringType(), True),
        StructField('device_custom_string', StringType(), True),
        StructField('device_direction', StringType(), True),
        StructField('device_dns_domain', StringType(), True),
        StructField('device_domain', StringType(), True),
        StructField('device_event_class_id', StringType(), True),
        StructField('device_external_id', StringType(), True),
        StructField('device_facility', StringType(), True),
        StructField('device_inbound_interface', StringType(), True),
        StructField('device_name', StringType(), True),
        StructField('device_nt_domain', StringType(), True),
        StructField('device_outbound_interface', StringType(), True),
        StructField('device_process_name', StringType(), True),
        StructField('device_product', StringType(), True),
        StructField('device_severity', StringType(), True),
        StructField('device_vendor', StringType(), True),
        StructField('device_version', StringType(), True),
        StructField('device_zone_name', StringType(), True),
        StructField('device_zone_uri', StringType(), True),
        StructField('dhost', StringType(), True),
        StructField('dntdom', StringType(), True),
        StructField('dpriv', StringType(), True),
        StructField('dproc', StringType(), True),
        StructField('dst', StringType(), True),
        StructField('dtz', StringType(), True),
        StructField('duid', StringType(), True),
        StructField('duser', StringType(), True),
        StructField('dvc', StringType(), True),
        StructField('dvchost', StringType(), True),
        StructField('dvcmac', StringType(), True),
        StructField('end', TimestampType(), True),
        StructField('event_count__slc_', StringType(), True),
        StructField('event_id', DecimalType(), True),
        StructField('event_source', StringType(), True),
        StructField('event_throughput', StringType(), True),
        StructField('event_throughput__slc_', StringType(), True),
        StructField('eventlog_category', StringType(), True),
        StructField('external_id', StringType(), True),
        StructField('file_create_time', TimestampType(), True),
        StructField('file_id', StringType(), True),
        StructField('file_modification_time', TimestampType(), True),
        StructField('file_path', StringType(), True),
        StructField('file_permission', StringType(), True),
        StructField('fsize', IntegerType(), True),
        StructField('file_type', StringType(), True),
        StructField('flex_date1', TimestampType(), True),
        StructField('flex_number1', IntegerType(), True),
        StructField('flex_number2', IntegerType(), True),
        StructField('flex_string1', StringType(), True),
        StructField('flex_string2', StringType(), True),
        StructField('fname', StringType(), True),
        StructField('geid', StringType(), True),
        StructField('generator', StringType(), True),
        StructField('generator_id', StringType(), True),
        StructField('impersonation_level', StringType(), True),
        StructField('last_time', StringType(), True),
        StructField('logon_guid', StringType(), True),
        StructField('logon_type', StringType(), True),
        StructField('message', StringType(), True),
        StructField('mrt', StringType(), True),
        StructField('msg', StringType(), True),
        StructField('name', StringType(), True),
        StructField('new_time', StringType(), True),
        StructField('object_type', StringType(), True),
        StructField('old_file_create_time', TimestampType(), True),
        StructField('old_file_hash', StringType(), True),
        StructField('old_file_modification_time', TimestampType(), True),
        StructField('old_file_name', StringType(), True),
        StructField('old_file_path', StringType(), True),
        StructField('old_file_permission', StringType(), True),
        StructField('old_file_size', IntegerType(), True),
        StructField('old_file_type', StringType(), True),
        StructField('orrelated_event_count', StringType(), True),
        StructField('package_name', StringType(), True),
        StructField('pre_authentication_type', StringType(), True),
        StructField('previous_time', StringType(), True),
        StructField('process_id', StringType(), True),
        StructField('raw_event', StringType(), True),
        StructField('raw_event_character_throughput', StringType(), True),
        StructField('raw_event_character_throughput__slc_', StringType(), True),
        StructField('raw_event_length__slc_', StringType(), True),
        StructField('reason', StringType(), True),
        StructField('request_context', StringType(), True),
        StructField('request_cookies', StringType(), True),
        StructField('request_method', StringType(), True),
        StructField('request_protocol', StringType(), True),
        StructField('request', StringType(), True),
        StructField('request_url_authority', StringType(), True),
        StructField('request_url_file_name', StringType(), True),
        StructField('request_url_host', StringType(), True),
        StructField('request_url_port', StringType(), True),
        StructField('request_url_query', StringType(), True),
        StructField('result_code', StringType(), True),
        StructField('rrequest_client_application', StringType(), True),
        StructField('rt', TimestampType(), False),
        StructField('shost', StringType(), True),
        StructField('source_asset_name', StringType(), True),
        StructField('source_dns_domain', StringType(), True),
        StructField('source_i_pv6_address', StringType(), True),
        StructField('smac', StringType(), True),
        StructField('sntdom', StringType(), True),
        StructField('sproc', StringType(), True),
        StructField('source_service_name', StringType(), True),
        StructField('suid', StringType(), True),
        StructField('spriv', StringType(), True),
        StructField('source_zone_name', StringType(), True),
        StructField('source_zone_uri', StringType(), True),
        StructField('spt', IntegerType(), True),
        StructField('src', StringType(), True),
        StructField('start', TimestampType(), True),
        StructField('status', StringType(), True),
        StructField('suser', StringType(), True),
        StructField('total_event_count', StringType(), True),
        StructField('total_raw_event_length', StringType(), True),
        StructField('proto', StringType(), True),
        StructField('type', StringType(), True),
        StructField('vulnerability', StringType(), True),
        StructField('vulnerability_id', StringType(), True),
        StructField('vulnerability_name', StringType(), True),
        StructField('vulnerability_resource', StringType(), True),
        StructField('parition_year', IntegerType(), True),
        StructField('parition_month', IntegerType(), True),
        StructField('parition_day', IntegerType(), True),
        StructField('agent_dns_domain', StringType(), True),
        StructField('agent_nt_domain', StringType(), True),
        StructField('agent_translated_address', StringType(), True),
        StructField('agent_translated_zone_external_id', StringType(), True),
        StructField('agent_translated_zone_uri', StringType(), True),
        StructField('agent_zone_external_id', StringType(), True),
        StructField('attacker_geo_country_code', StringType(), True),
        StructField('attacker_geo_location_info', StringType(), True),
        StructField('attacker_geo_postal_code', StringType(), True),
        StructField('attacker_geo_region_code', StringType(), True),
        StructField('attacker_translated_zone_external_id', StringType(), True),
        StructField('attacker_translated_zone_uri', StringType(), True),
        StructField('c6a1', StringType(), True),
        StructField('c6a1label', StringType(), True),
        StructField('c6a2', StringType(), True),
        StructField('c6a2label', StringType(), True),
        StructField('c6a3', StringType(), True),
        StructField('c6a3label', StringType(), True),
        StructField('c6a4', StringType(), True),
        StructField('c6a4label', StringType(), True),
        StructField('category_technique', StringType(), True),
        StructField('category_tuple_description', StringType(), True),
        StructField('cn1label', StringType(), True),
        StructField('cn2label', StringType(), True),
        StructField('cn3label', StringType(), True),
        StructField('crypto_signature', StringType(), True),
        StructField('cs6', StringType(), True),
        StructField('cs6label', StringType(), True),
        StructField('customer_external_id', StringType(), True),
        StructField('customer_uri', StringType(), True),
        StructField('destination_geo_country_code', StringType(), True),
        StructField('destination_geo_location_info', StringType(), True),
        StructField('destination_geo_postal_code', StringType(), True),
        StructField('destination_geo_region_code', StringType(), True),
        StructField('destination_translated_address', StringType(), True),
        StructField('destination_translated_port', IntegerType(), True),
        StructField('destination_translated_zone_external_id', StringType(), True),
        StructField('destination_translated_zone_uri', StringType(), True),
        StructField('destination_zone_external_id', StringType(), True),
        StructField('device_custom_date1label', StringType(), True),
        StructField('device_custom_date2label', StringType(), True),
        StructField('device_payload_id', StringType(), True),
        StructField('device_process_name_', StringType(), True),
        StructField('device_translated_address', StringType(), True),
        StructField('device_translated_zone_external_id', StringType(), True),
        StructField('device_translated_zone_uri', StringType(), True),
        StructField('device_zone_external_id', StringType(), True),
        StructField('dlat', DecimalType(), True),
        StructField('dlong', DecimalType(), True),
        StructField('dmac', StringType(), True),
        StructField('dpid', IntegerType(), True),
        StructField('dvcpid', IntegerType(), True),
        StructField('file_hash', StringType(), True),
        StructField('flex_date1label', StringType(), True),
        StructField('flex_number1label', StringType(), True),
        StructField('flex_number2label', StringType(), True),
        StructField('flex_string1label', StringType(), True),
        StructField('flex_string2label', StringType(), True),
        StructField('generator_external_id', StringType(), True),
        StructField('generator_uri', StringType(), True),
        StructField('old_file_id', StringType(), True),
        StructField('outcome', StringType(), True),
        StructField('request_client_application', StringType(), True),
        StructField('slat', DecimalType(), True),
        StructField('slong', DecimalType(), True),
        StructField('source_geo_country_code', StringType(), True),
        StructField('source_geo_location_info', StringType(), True),
        StructField('source_geo_postal_code', StringType(), True),
        StructField('source_geo_region_code', StringType(), True),
        StructField('source_translated_address', StringType(), True),
        StructField('source_translated_port', IntegerType(), True),
        StructField('source_translated_zone_external_id', StringType(), True),
        StructField('source_translated_zone_uri', StringType(), True),
        StructField('source_zone_external_id', StringType(), True),
        StructField('spid', IntegerType(), True),
        StructField('target_geo_country_code', StringType(), True),
        StructField('target_geo_location_info', StringType(), True),
        StructField('target_geo_postal_code', StringType(), True),
        StructField('target_geo_region_code', StringType(), True),
        StructField('target_translated_zone_uri', StringType(), True),
        StructField('vulnerability_external_id', StringType(), True),
        StructField('vulnerability_uri', StringType(), True)
    ])
    main_def_for_spark(temp_dir, result_dir, hdfs_url, sc, sqlContext, schema, parsing_function_ArcSigth)


