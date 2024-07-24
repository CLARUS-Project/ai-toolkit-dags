"""
This module defines the `select_best_model` function used by the pipeline orchestrator to select the best model 
from an MLflow experiment and change the tag to production.

Any additional functions or utilities required for this step can be defined within this script itself or split 
into different scripts and included in the Process directory.
"""

import mlflow
from mlflow.tracking.client import MlflowClient
import config

def select_best_model():
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

    # Search for all registered models
    registered_models = client.search_registered_models()

    # Filter for models in the Production stage
    production_models = [
        (model.name, version) 
        for model in registered_models 
        for version in model.latest_versions 
        if version.current_stage == "Production"
    ]

    if not production_models:
        raise Exception("No production models found")

    # Sort by creation timestamp to get the most recent one
    latest_model_name, latest_version = max(production_models, key=lambda x: x[1].last_updated_timestamp)

    # Get the new model version
    new_model_version = client.get_latest_versions(name=latest_model_name, stages=["None"])[-1]

    # Archive all models in production
    for model_name, version in production_models:
        client.transition_model_version_stage(
            name=model_name,
            version=version.version,
            stage="Archived"
        )
        print(f'Model version {version.version} of {model_name} has been archived.')

    # Transition the latest model version to production
    client.transition_model_version_stage(
        name=latest_model_name,
        version=new_model_version.version,
        stage='Production',
        archive_existing_versions=True
    )

    print(f"New model version {latest_version.version} transitioned to production")

    # Retrieve the run associated with the new model version
    best_run_id = latest_version.run_id
    run = mlflow.get_run(best_run_id)
    artifact_path = run.info.artifact_uri + '/model'
    model_metrics = run.data.metrics

    print(f'best_run: {best_run_id}, artifact_path: {artifact_path}, model_metrics: {model_metrics}')

    return {'best_run': best_run_id, 'artifact_path': artifact_path, 'model_metrics': model_metrics}
