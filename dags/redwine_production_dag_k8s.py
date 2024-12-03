"""
This module defines the Airflow DAG for the Red Wine MLOps lifecycle. The DAG includes tasks
for various stages of the pipeline, including data reading, data processing, model training, 
and selecting the best model. 

The tasks are defined as functions and executed within the DAG. The execution order of the tasks 
is defined using task dependencies.

Note: The actual logic inside each task is not shown in the code, as it may reside in external 
script files.

The DAG is scheduled to run every day at 12:00 AM.

Please ensure that the necessary dependencies are installed and accessible for executing the tasks.

test
"""

from datetime import datetime
from airflow.decorators import dag, task
from kubernetes.client import models as k8s
from airflow.models import Variable

@dag(
    description='MLOps lifecycle production',
    schedule_interval=None, 
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['demo', 'redWine_production'],
) 
def redwine_production_dag_over_k8s():

    env_vars={
        "POSTGRES_USERNAME": Variable.get("POSTGRES_USERNAME"),
        "POSTGRES_PASSWORD": Variable.get("POSTGRES_PASSWORD"),
        "POSTGRES_DATABASE": Variable.get("POSTGRES_DATABASE"),
        "POSTGRES_HOST": Variable.get("POSTGRES_HOST"),
        "POSTGRES_PORT": Variable.get("POSTGRES_PORT"),
        "TRUE_CONNECTOR_EDGE_IP": Variable.get("CONNECTOR_EDGE_IP"),
        "TRUE_CONNECTOR_EDGE_PORT": Variable.get("IDS_EXTERNAL_ECC_IDS_PORT"),
        "TRUE_CONNECTOR_CLOUD_IP": Variable.get("CONNECTOR_CLOUD_IP"),
        "TRUE_CONNECTOR_CLOUD_PORT": Variable.get("IDS_PROXY_PORT"),
        "MLFLOW_ENDPOINT": Variable.get("MLFLOW_ENDPOINT"),
        "MLFLOW_TRACKING_USERNAME": Variable.get("MLFLOW_TRACKING_USERNAME"),
        "MLFLOW_TRACKING_PASSWORD": Variable.get("MLFLOW_TRACKING_PASSWORD"),
        "container": "docker"
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
        command=["sh", "-c", "mkdir -p /git && cd /git && git clone -b red_wine_example_production --single-branch https://github.com/CLARUS-Project/ai-toolkit-dags.git"],
        volume_mounts=init_container_volume_mounts
    )

    # Define as many task as needed
    @task.kubernetes(
        image='clarusproject/dag-image:1.0.0-slim',
        name='read_data',
        task_id='read_data',
        namespace='airflow',
        init_containers=[init_container],
        volumes=[volume],
        volume_mounts=[volume_mount],
        do_xcom_push=True,
        env_vars=env_vars
    )
    def read_data_procces_task():
        import sys
        import redis
        import uuid
        import pickle

        sys.path.insert(1, '/git/ai-toolkit-dags/src/redwine')
        from Data.read_data import read_data
        from Process.data_processing import data_processing

        redis_client = redis.StrictRedis(
            host='redis-headless.redis.svc.cluster.local',
            port=6379,  # El puerto por defecto de Redis
            password='pass'
        )

        df = read_data()
        dp = data_processing(df)

        read_id = str(uuid.uuid4())

        redis_client.set('data-' + read_id, pickle.dumps(dp))

        return read_id
    

    @task.kubernetes(
        image='clarusproject/dag-image:1.0.0-slim',
        name='model_retraining',
        task_id='model_retraining',
        namespace='airflow',
        get_logs=True,
        init_containers=[init_container],
        volumes=[volume],
        volume_mounts=[volume_mount],
        env_vars=env_vars,
        do_xcom_push=True

    )
    def model_retraining_result_task(read_id=None):
        import sys
        import redis
        import pickle

        sys.path.insert(1, '/git/ai-toolkit-dags/src/redwine')
        from Models.model_retrain import model_retrain

        redis_client = redis.StrictRedis(
            host='redis-headless.redis.svc.cluster.local',
            port=6379,  # El puerto por defecto de Redis
            password='pass'
        )

        data = redis_client.get('data-' + read_id)
        res = pickle.loads(data)
        retrain_info = model_retrain(res)

        redis_client = redis.StrictRedis(
            host='redis-headless.redis.svc.cluster.local',
            port=6379,  # El puerto por defecto de Redis
            password='pass'
        )

        redis_client.delete('data-' + read_id)

        return retrain_info
    
    @task.kubernetes(
        image='clarusproject/dag-image:1.0.0-slim',
        name='select_best_model',
        task_id='select_best_model',
        namespace='airflow',
        get_logs=True,
        init_containers=[init_container],
        volumes=[volume],
        volume_mounts=[volume_mount],
        env_vars=env_vars,
        do_xcom_push=True
    )
    def select_best_model_task(retrain_info):
        import sys

        sys.path.insert(1, '/git/ai-toolkit-dags/src/redwine')
        from Deployment.select_best_model import select_best_model

        return select_best_model(retrain_info)
    
    @task.kubernetes(
        image='clarusproject/dag-image:1.0.0-slim',
        name='register_experiment',
        task_id='register_experiment',
        namespace='airflow',
        get_logs=True,
        init_containers=[init_container],
        volumes=[volume],
        volume_mounts=[volume_mount],
        env_vars=env_vars
    )
    def register_experiment_task(best_model_res):
        import sys

        sys.path.insert(1, '/git/ai-toolkit-dags/src/redwine')
        from Deployment.register_experiment import register_experiment_rds

        return register_experiment_rds(best_model_res)
    
    @task.kubernetes(
        image='mfernandezlabastida/kaniko:1.0',
        name='build_inference',
        task_id='build_inference',
        namespace='airflow',
        init_containers=[init_container],
        volumes=[volume],
        volume_mounts=[volume_mount],
        do_xcom_push=True,
        container_resources=k8s.V1ResourceRequirements(
            requests={'cpu': '0.5'},
            limits={'cpu': '1.5'}
        ),
        priority_class_name='medium-priority',
        env_vars=env_vars
    )
    def build_inference_task(run_id):
        import mlflow
        import os
        import logging
        import subprocess

        """
        MODIFY WHAT YOU WANT
        """
        path = '/git/ai-toolkit-dags/build_docker'
        endpoint = 'registry-docker-registry.registry.svc.cluster.local:5001/redwine:prod'


        def download_artifacts(run_id, path):
            mlflow.set_tracking_uri("http://mlflow-tracking.mlflow.svc.cluster.local:5000")

            local_path = mlflow.artifacts.download_artifacts(run_id=run_id, dst_path=path)

            # Buscar el archivo model.pkl y moverlo a la carpeta local_path en caso de que se encuentre en una subcarpeta
            for root, dirs, files in os.walk(local_path):
                for file in files:
                    if file.startswith("model"):
                        logging.info(f"Encontrado archivo model.pkl en: {root}")
                        os.rename(os.path.join(root, file), os.path.join(local_path + '/model', file))
                    elif file.startswith("requirements"):
                        logging.info(f"Encontrado archivo requirements.txt en: {root}")
                        os.rename(os.path.join(root, file), os.path.join(path, file))

        def modify_requirements_file(path):
            required_packages = ["fastapi", "uvicorn", "pydantic", "numpy"]

            with open(f"{path}/requirements.txt", "r") as f:
                lines = f.readlines()

            with open(f"{path}/requirements.txt", "w") as f:
                for line in lines:
                    if line.strip() not in required_packages:
                        f.write(line)

                f.write("\n")

                for package in required_packages:
                    f.write(f"{package}\n")


        logging.warning(f"Downloading artifacts from run_id: {run_id['best_run']}")
        download_artifacts(run_id['best_run'], path)
        modify_requirements_file(path)

        args = [
            "/kaniko/executor",
            f"--dockerfile={path}/Dockerfile",
            f"--context={path}",
            f"--destination={endpoint}",
            f"--cache=false"
        ]
        result = subprocess.run(
            args,
            check=True  # Lanza una excepción si el comando devuelve un código diferente de cero
        )
        logging.warning(f"Kaniko executor finished with return code: {result.returncode}")
    

    # Instantiate each task and define task dependencies
    processing_result = read_data_procces_task()
    model_retraining_result = model_retraining_result_task(processing_result)
    select_best_model_result = select_best_model_task(model_retraining_result)
    register_experiment_result = register_experiment_task(select_best_model_result)
    build_inference_result = build_inference_task(select_best_model_result)

    # Define the order of the pipeline
    processing_result >> model_retraining_result >> select_best_model_result >> [register_experiment_result, build_inference_result]

# Call the DAG 
redwine_production_dag_over_k8s()
