"""
This file represents the schema that must be followed to correctly implement a dag running on kubernetes.

This file specifies the values that need to be modified to adapt it to any need.

Only simple variables such as an int, string or dict can be passed between tasks.
If you want to pass something more complex like a dataframe you need to use Redis.

The code you want to execute has to be placed where indicated.
"""

from datetime import datetime
from airflow.decorators import dag, task
from kubernetes.client import models as k8s

@dag(
    description=<DESCRIPTION>,
    schedule_interval=<SCHEDULE>, 
    start_date=<START_DATE>,
    catchup=<CATCHUP>, # True or false
    tags=<ARRAY_OF_TAGS>, # Not necessary
)
def dag_structure():

    """
    CONFIGURATION OF THE EXECUTING CONTAINER, just change the 'TODO' 
    """
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
        # TODO CHANGE THE REPOSITORY, this is needed in case the dag uses custom imports
        # In the case of a private repository, a token can be generated at project level
        # and follow this scheme https://<repo_name>:<project_token>@gitlab.com/path/to/repo.git
        command=["sh", "-c", "mkdir -p /git && cd /git && git clone -b <BRANCH_NAME> --single-branch <YOUR REPOSITORY>"], 
        volume_mounts=init_container_volume_mounts
    )

    """
    DEFINITION OF THE TASKS, as many as necessary
    """
    @task.kubernetes(
        env_vars={"VAR_1": "var1", "VAR_2": "var2"}
        #TODO CHANGE THE IMAGE, the task will be executed on this image
        # Currently only implemented for public images.
        image='<YOUR DOCKER IMAGE>',
        #TODO CHANGE THE NAME AND ID
        name='<TASK NAME>',
        task_id='<TASK ID>',
        namespace='airflow', # Do not change
        get_logs=True,
        init_containers=[init_container], # Do not change
        volumes=[volume], # Do not change
        volume_mounts=[volume_mount], # Do not change
        # In case this task returns a value that is then passed to another task set this option to TRUE.
        do_xcom_push=False,
        # node_selector={"device": "DGX"}, # UNIMPLEMENTED FUNCTIONALITY
    )
    def task_1():
        import sys
        import redis

        """
        IMPORT FUNCTIONS FROM THE REPOSITORY ITSELF
        """
        sys.path.insert(1, '/git/<YOUR_REPO_NAME>/<PATH_TO_SRC>')
        from Data.read_data import read_data
        from Process.data_processing import data_processing


        """
        CODE TO USE REDIS
        """
        redis_client = redis.StrictRedis(
            host='redis-headless.redis.svc.cluster.local',
            port=6379,
            password='pass'
        )

        redis_client.set('<KEY>', <VALUE>)
        data = redis_client.get('<KEY>')
        redis_client.delete('<KEY>')


        """
        YOUR CODE GOES HERE
        """

        return 


    @task.kubernetes(
        #TODO CHANGE THE IMAGE, the task will be executed on this image
        # Currently only implemented for public images.
        image='<YOUR DOCKER IMAGE>',
        #TODO CHANGE THE NAME AND ID
        name='<TASK NAME>',
        task_id='<TASK ID>',
        namespace='airflow', # Do not change
        get_logs=True,
        init_containers=[init_container], # Do not change
        volumes=[volume], # Do not change
        volume_mounts=[volume_mount], # Do not change
        # In case this task returns a value that is then passed to another task set this option to TRUE.
        do_xcom_push=False,
        # node_selector={"device": "DGX"}, # UNIMPLEMENTED FUNCTIONALITY
    )
    def task_2(read_id=None):
        import sys
        import redis

        """
        IMPORT FUNCTIONS FROM THE REPOSITORY ITSELF
        """
        sys.path.insert(1, '/git/dags/<YOUR_REPO_NAME>/<PATH_TO_SRC>')
        from Data.read_data import read_data
        from Process.data_processing import data_processing


        """
        CODE TO USE REDIS
        """
        redis_client = redis.StrictRedis(
            host='redis-headless.redis.svc.cluster.local',
            port=6379,
            password='pass'
        )

        redis_client.set('<KEY>', <VALUE>)
        data = redis_client.get('<KEY>')
        redis_client.delete('<KEY>')


        """
        YOUR CODE GOES HERE
        """

        return 
    

    # Instantiate each task and define task dependencies
    task_1_result = task_1()
    task_2_result = task_2(task_1_result)

    # Define the order of the pipeline, sequentially
    task_1_result >> task_2_result

    # Define the order of the pipeline, in parallel
    [task_1_result, task_2_result]

dag_structure()