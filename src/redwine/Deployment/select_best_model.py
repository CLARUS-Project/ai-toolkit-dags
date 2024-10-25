"""
This module defines the `select_best_model` function used by the pipeline orchestrator to select the best model 
from an MLflow experiment and change the tag to production.

Any additional functions or utilities required for this step can be defined within this script itself or split 
into different scripts and included in the Process directory.
"""

import mlflow
from mlflow.tracking.client import MlflowClient
import config

def select_best_model(retrain_info: dict = None):
    """
    Select the latest version of the retrained model and change the tag to production

    Returns:
        None
    """

    endpoint = config.MLFLOW_ENDPOINT
    experiment = config.MLFLOW_EXPERIMENT

    client = MlflowClient(endpoint)
    mlflow.set_tracking_uri(endpoint)
    mlflow.set_experiment(experiment)  
    print(retrain_info)


    latest_model_name = retrain_info['latest_model_name']
    new_version = retrain_info['new_version']

    # Archive all models in production
    production_models = client.get_latest_versions(name=latest_model_name, stages=["Production"])
    for version in production_models:
        client.transition_model_version_stage(
            name=latest_model_name,
            version=version.version,
            stage="Archived"
        )
        print(f'Model version {version.version} of {latest_model_name} has been archived.')

    # Transition the new model version to production
    client.transition_model_version_stage(
        name=latest_model_name,
        version=new_version,
        stage='Production',
        archive_existing_versions=True
    )

    print(f"New model version {new_version} transitioned to production")

    # Retrieve the run associated with the new model version
    best_run_id = retrain_info['new_run_id']
    run = mlflow.get_run(best_run_id)
    artifact_path = run.info.artifact_uri + '/model'
    model_metrics = run.data.metrics

    print(f'Best run: {best_run_id}, artifact_path: {artifact_path}, model_metrics: {model_metrics}')

    return {'best_run': best_run_id, 'artifact_path': artifact_path, 'model_metrics': model_metrics}
