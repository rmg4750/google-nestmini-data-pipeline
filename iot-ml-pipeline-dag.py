from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from datetime import datetime, timedelta
import pandas as pd
import boto3

def preprocess_data():

    raw = pd.read_csv("/mnt/c/Users/Ryan/Documents/GoogleNestMLPipeline/data/processed/extracted_features_cicflowmeter.csv")
    iot = pd.read_csv("/mnt/c/Users/Ryan/Documents/IoT Anomaly Detection Project/RT_IOT2022.csv")


    column_mapping = {
        'id.orig_p': 'src_port',
        'id.resp_p': 'dst_port',
        'proto': 'protocol',
        'flow_duration': 'flow_duration',
        'fwd_pkts_tot': 'tot_fwd_pkts',
        'bwd_pkts_tot': 'tot_bwd_pkts',
        'fwd_data_pkts_tot': 'totlen_fwd_pkts',
        'bwd_data_pkts_tot': 'totlen_bwd_pkts',
        'fwd_pkts_per_sec': 'fwd_pkts_s',
        'bwd_pkts_per_sec': 'bwd_pkts_s',
        'flow_pkts_per_sec': 'flow_pkts_s',
        'down_up_ratio': 'down_up_ratio',
        'fwd_header_size_tot': 'fwd_header_len',
        'bwd_header_size_tot': 'bwd_header_len',
        'fwd_PSH_flag_count': 'fwd_psh_flags',
        'bwd_PSH_flag_count': 'bwd_psh_flags',
        'flow_FIN_flag_count': 'fin_flag_cnt',
        'flow_SYN_flag_count': 'syn_flag_cnt',
        'flow_RST_flag_count': 'rst_flag_cnt',
        'flow_ACK_flag_count': 'ack_flag_cnt',
        'fwd_URG_flag_count': 'fwd_urg_flags',
        'bwd_URG_flag_count': 'bwd_urg_flags',
        'fwd_iat.min': 'fwd_iat_min',
        'fwd_iat.max': 'fwd_iat_max',
        'fwd_iat.avg': 'fwd_iat_mean',
        'fwd_iat.std': 'fwd_iat_std',
        'bwd_iat.min': 'bwd_iat_min',
        'bwd_iat.avg': 'bwd_iat_mean',
        'bwd_iat.std': 'bwd_iat_std',
        'flow_iat.min': 'flow_iat_min',
        'flow_iat.max': 'flow_iat_max',
        'flow_iat.avg': 'flow_iat_mean',
        'flow_iat.std': 'flow_iat_std',
        'active.min': 'active_min',
        'active.max': 'active_max',
        'active.avg': 'active_mean',
        'active.std': 'active_std',
        'idle.min': 'idle_min',
        'idle.max': 'idle_max',
        'idle.avg': 'idle_mean',
        'fwd_subflow_pkts': 'subflow_fwd_pkts',
        'bwd_subflow_pkts': 'subflow_bwd_pkts',
        'fwd_subflow_bytes': 'subflow_fwd_byts',
        'bwd_subflow_bytes': 'subflow_bwd_byts'
    }



    iot.rename(columns=column_mapping, inplace=True)
    
    raw["Attack_type"] = "Google_Nest_Mini"


    common_columns = list(set(iot.columns).intersection(set(raw.columns)))

    iot_filtered = iot[common_columns]
    raw_filtered = raw[common_columns]

    iot_filtered.to_csv("/mnt/c/Users/Ryan/Documents/GoogleNestMLPipeline/data/processed/processed_iot.csv", index=False)
    raw_filtered.to_csv("/mnt/c/Users/Ryan/Documents/GoogleNestMLPipeline/data/processed/processed_nestmini.csv", index=False)
    
def send_traffic_data():
    raw = pd.read_csv("/mnt/c/Users/Ryan/Documents/GoogleNestMLPipeline/data/processed/extracted_features_cicflowmeter.csv")
    traffic = raw[['timestamp', 'tot_fwd_pkts', 'tot_bwd_pkts']]
    traffic['total_packets'] = traffic['tot_fwd_pkts'] + traffic['tot_bwd_pkts']
    
    packet_count_by_time = traffic.groupby('timestamp')['total_packets'].sum().reset_index()
    packet_count_by_time.to_csv("/mnt/c/Users/Ryan/Documents/GoogleNestMLPipeline/data/processed/traffic_volume.csv", index=False)
    
def upload_to_s3(file_path, s3_bucket, s3_key):
    s3 = boto3.client('s3')
    s3.upload_file(file_path, s3_bucket, s3_key)

with DAG("iot-ml-pipeline-dag", start_date= datetime(2024, 9, 23),
         schedule_interval=timedelta(seconds=45), catchup=False ) as dag:
    
    run_tshark = BashOperator(
        task_id='run_tshark',
        bash_command='''
        powershell.exe -Command "cd C:\\Users\\Ryan; tshark -i \\"\\Device\\NPF_{0F30EA67-69DA-4B34-9A1F-A239B58FB76A}\\" -f \\"host 192.168.1.48\\" -a duration:30 -w \\"C:\\Users\\Ryan\\Documents\\GoogleNestMLPipeline\\data\\raw\\capture.pcapng\\""
        '''
    )
    
    upload_raw_to_s3 = PythonOperator(
        task_id='upload_raw_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'file_path': '/mnt/c/Users/Ryan/Documents/GoogleNestMLPipeline/data/raw/capture.pcapng',
            's3_bucket': 'my-nestmini-data',
            's3_key': 'raw_data/capture.pcapng'
        }
    )
    
    run_cicflowmeter_task = BashOperator(
    task_id='run_cicflowmeter',
    bash_command='cicflowmeter -f /mnt/c/Users/Ryan/Documents/GoogleNestMLPipeline/data/raw/capture.pcapng -c /mnt/c/Users/Ryan/Documents/GoogleNestMLPipeline/data/processed/extracted_features_cicflowmeter.csv'
    )
    
    upload_feature_extracted_to_s3 = PythonOperator(
        task_id='upload_feature_extracted_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'file_path': '/mnt/c/Users/Ryan/Documents/GoogleNestMLPipeline/data/raw/capture.pcapng',
            's3_bucket': 'my-nestmini-data',
            's3_key': 'feature_extracted_data/extracted_features.csv'
        }
    )
    
    preprocess_task = PythonOperator(
        task_id='preprocess_data_task',
        python_callable=preprocess_data,
    )
    
    upload_processed_to_s3 = PythonOperator(
        task_id='upload_processed_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'file_path': '/mnt/c/Users/Ryan/Documents/GoogleNestMLPipeline/data/processed/processed_nestmini.csv',
            's3_bucket': 'my-nestmini-data',
            's3_key': 'processed_data/processed_nestmini.csv'
        }
    )
    
    traffic_volume_task = PythonOperator(
        task_id ='traffic_volume_task',
        python_callable=send_traffic_data
    )
    
    load_data_to_redshift = S3ToRedshiftOperator(
        task_id='load_data_to_redshift',
        schema='public',
        table='nestmini',
        s3_bucket='my-nestmini-data',
        s3_key='processed_data/processed_nestmini.csv',
        copy_options=['CSV', 'IGNOREHEADER 3'],
        autocommit=True,
        aws_conn_id='aws_default',
        redshift_conn_id='my_redshift'
    )
    
    
    
    run_tshark >> upload_raw_to_s3 >> run_cicflowmeter_task >> upload_feature_extracted_to_s3 >> preprocess_task >> upload_processed_to_s3 >> load_data_to_redshift
    run_tshark >> run_cicflowmeter_task >> traffic_volume_task
    
