import json
import pandas as pd
import numpy as np
from evidently.pipeline.column_mapping import ColumnMapping
from evidently.model_profile import Profile
from evidently.model_profile.sections import DataDriftProfileSection
import mlflow


def detect_dataset_drift(reference, production, column_mapping, confidence=0.95, threshold=0.5, get_ratio=False):
    data_drift_profile = Profile(sections=[DataDriftProfileSection()])
    data_drift_profile.calculate(
        reference, production, column_mapping=column_mapping)
    data_drift_profile.calculate(reference, production, column_mapping=column_mapping)
    report = data_drift_profile.json()
    json_report = json.loads(report)