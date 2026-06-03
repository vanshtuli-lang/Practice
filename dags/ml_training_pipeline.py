"""
End-to-end ML training pipeline.

Triggers when fresh raw training data lands, validates and engineers features,
fans out hyperparameter tuning across three learning rates in parallel, then
evaluates the candidates on a GPU pod and promotes the winner to the model registry.
"""

from pendulum import datetime

from airflow.sdk import dag, task, Asset

# Assets are the Airflow 3 contract for data products — upstream jobs "produce" them,
# downstream DAGs subscribe to them. No cron, no polling, no guesswork.
raw_training_data = Asset("s3://ml-platform/raw/training_events.parquet")
validated_features = Asset("s3://ml-platform/features/validated.parquet")
model_registry = Asset("mlflow://models/churn_classifier")


@dag(
    start_date=datetime(2026, 1, 1),
    schedule=[raw_training_data],  # Fires the instant the upstream raw asset is refreshed
    catchup=False,
    tags=["ml", "demo", "assets", "dynamic-mapping"],
)
def ml_training_pipeline():

    @task(outlets=[validated_features])
    def prepare_features():
        # In prod this would be a Spark/Snowpark job — mocked here for the demo
        print("Pulling raw training events, running schema validation + feature engineering")
        features = {"rows": 1_250_000, "feature_count": 87, "path": validated_features.uri}
        print(f"Wrote validated feature set: {features}")
        return features

    @task
    def train_model(learning_rate: float, features: dict) -> dict:
        # Each mapped instance is its own task run — parallel, observable, retryable
        print(f"Training churn_classifier with lr={learning_rate} on {features['rows']:,} rows")
        # Mock accuracy that conveniently peaks at lr=0.05 for a clean demo narrative
        mock_accuracy = 0.91 - abs(learning_rate - 0.05) * 2
        return {"learning_rate": learning_rate, "accuracy": round(mock_accuracy, 4)}

    @task(
        outlets=[model_registry],
        # Request a GPU pod just for the eval step — keeps the rest of the cluster cheap
        executor_config={
            "pod_override": {
                "spec": {
                    "containers": [{
                        "name": "base",
                        "resources": {"limits": {"nvidia.com/gpu": 1}},
                    }]
                }
            }
        },
    )
    def evaluate_and_register(candidates: list[dict]):
        winner = max(candidates, key=lambda c: c["accuracy"])
        print(f"Evaluated {len(candidates)} candidate models on GPU pod")
        print(f"Winner: lr={winner['learning_rate']} | accuracy={winner['accuracy']}")
        print(f"Promoting to {model_registry.uri} — downstream serving DAGs will trigger automatically")
        return winner

    features = prepare_features()
    # Fan out training across learning rates with one line — no manual loop, no hardcoded count
    candidates = train_model.partial(features=features).expand(
        learning_rate=[0.01, 0.05, 0.1]
    )
    evaluate_and_register(candidates)


ml_training_pipeline()
