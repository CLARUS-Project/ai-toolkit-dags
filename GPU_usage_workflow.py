"""
Esto es un test
"""

from datetime import datetime
from airflow.decorators import dag, task
from kubernetes.client import models as k8s
from airflow.models import Variable
# from kubernetes.client.models import V1Pod, V1ObjectMeta, V1PodSpec, V1Container

@dag(
    description='test2',
    schedule_interval='* 12 * * *', 
    start_date=datetime.now(),
    catchup=False,
    tags=['demo', 'GPU'],
)
def GPU_usage_workflow():

    env_vars={
        "POSTGRES_USERNAME": Variable.get("POSTGRES_USERNAME"),
        "POSTGRES_PASSWORD": Variable.get("POSTGRES_PASSWORD"),
        "POSTGRES_DATABASE": Variable.get("POSTGRES_DATABASE"),
        "POSTGRES_HOST": Variable.get("POSTGRES_HOST"),
        "POSTGRES_PORT": Variable.get("POSTGRES_PORT"),
        "TRUE_CONNECTOR_EDGE_IP": Variable.get("CONNECTOR_EDGE_IP"),
        "TRUE_CONNECTOR_EDGE_PORT": Variable.get("IDS_EXTERNAL_ECC_IDS_PORT"),
        "TRUE_CONNECTOR_CLOUD_IP": Variable.get("CONNECTOR_CLOUD_IP"),
        "TRUE_CONNECTOR_CLOUD_PORT": Variable.get("IDS_PROXY_PORT")
    }

    volume_mount = k8s.V1VolumeMount(
        name="dag-dependencies", mount_path="/git"
    )

    init_container_volume_mounts = [
        k8s.V1VolumeMount(mount_path="/git", name="dag-dependencies")
    ]

    volume = k8s.V1Volume(name="dag-dependencies", empty_dir=k8s.V1EmptyDirVolumeSource())

    init_container = k8s.V1Container(
        name="git-clone",
        image="alpine/git:latest",
        command=["sh", "-c", "mkdir -p /git && cd /git && git clone -b gpu_example --single-branch https://github.com/CLARUS-Project/ai-toolkit-dags.git"],
        volume_mounts=init_container_volume_mounts
    )

    pod_spec = k8s.V1Pod(
        api_version='v1',
        kind='Pod',
        spec=k8s.V1PodSpec(
            runtime_class_name='nvidia',  # Establecer runtimeClassName a 'nvidia'
            containers=[
                k8s.V1Container(
                    name='base',
                )
            ]
        )
    )

    @task.kubernetes(
        image='mfernandezlabastida/gpu_test:0.2',
        name='train_GPU',
        task_id='train_GPU',
        namespace='airflow', 
        init_containers=[init_container],
        image_pull_policy='Always',
        volumes=[volume],
        volume_mounts=[volume_mount],
        full_pod_spec=pod_spec,
        do_xcom_push=True,
        container_resources=k8s.V1ResourceRequirements(
            requests={'cpu': '1', 'nvidia.com/gpu': '1'},
            limits={'cpu': '1.5', 'nvidia.com/gpu': '1'}
        ),
        priority_class_name='high-priority',
        env_vars=env_vars
    )
    def train_GPU_task():
        import sys
        import time
    
        sys.path.insert(1, '/git/Clarus-Test')
        from train import train_and_evaluate
        
        # redis_client = redis.StrictRedis(
        #     host='redis-headless.redis.svc.cluster.local',
        #     port=6379,
        #     password='pass'
        # )
    
        return train_and_evaluate(device_type='cuda')

    @task.kubernetes(
        image='mfernandezlabastida/gpu_test:0.2',
        name='train_CPU',
        task_id='train_CPU',
        namespace='airflow', 
        init_containers=[init_container],
        image_pull_policy='Always',
        volumes=[volume],
        volume_mounts=[volume_mount],
        # full_pod_spec=pod_spec,
        container_resources=k8s.V1ResourceRequirements(
            requests={'cpu': '1'},
            limits={'cpu': '1'}
        ),
        do_xcom_push=True,
        env_vars=env_vars
    )
    def train_CPU_task():
        import sys
        import time
    
        sys.path.insert(1, '/git/Clarus-Test')
        from train import train_and_evaluate
        
        # redis_client = redis.StrictRedis(
        #     host='redis-headless.redis.svc.cluster.local',
        #     port=6379,
        #     password='pass'
        # )
    
        return train_and_evaluate(device_type='cpu')
    
    train_GPU_result = train_GPU_task()
    train_CPU_result = train_CPU_task()
    
    # Define the order of the pipeline
    [train_GPU_result, train_CPU_result]
# Call the DAG 
GPU_usage_workflow()