from typing import Dict, Any
from datetime import datetime
import mlflow
from mlflow.tracking.client import MlflowClient
import config
from Models import utils

def model_retrain(data: Dict[str, Any]):
    """
    Retrain the best model obtained using the provided data and move it to production.

    Args:
        data: A dictionary containing the preprocessed data.

    Returns:
        None
    """

    train_x = data['train_x']
    train_y = data['train_y']
    val_x = data['val_x']
    val_y = data['val_y']

    print('Execution init datetime: ' + str(datetime.now()))

    # Access to the best model obtained
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

    best_model_uri = latest_version.source
    best_model = mlflow.sklearn.load_model(best_model_uri)

    # Extract model parameters and name
    estimator_name = best_model.__class__.__name__    # Fetch relevant hyperparameters from the previous run
    relevant_hyperparams = client.get_run(latest_version.run_id).data.params
    hyperparams = {k: best_model.get_params()[k] for k in relevant_hyperparams}

    # Model retraining
    best_model.fit(train_x, train_y)

    # Model retraining performance evaluation
    training_metrics = utils.eval_metrics(train_y, best_model.predict(train_x), 'train')
    validation_metrics = utils.eval_metrics(val_y, best_model.predict(val_x), 'validation')

    # Track the run
    new_run = utils.track_run(latest_model_name,estimator_name,hyperparams,training_metrics,validation_metrics,best_model)

    # Find the newly registered version of the model
    new_model_version = client.get_latest_versions(name=latest_model_name, stages=["None"])[-1]  # Gets the latest version just registered

    retrain_info = {
        'latest_model_name': latest_model_name,
        'new_version': new_model_version.version,  # This is the new version just registered
        'new_run_id': new_run.info.run_id  # The run ID of the new run
    }

    print(retrain_info)
    # Return relevant information for the next step (new model version and run ID)
    return retrain_info
