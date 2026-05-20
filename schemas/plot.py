from pydantic import BaseModel
from datetime import datetime
from typing import Optional

class Plot(BaseModel):
    bucket_prefix: Optional[str] = ""
    data_collection: str
    data_type: str
    date: str
    durationOfCollection: Optional[str] = ""
    environment: str
    field: str
    gcs_dir_path: Optional[str] = ""
    genotype: str
    growth_stage: str
    ground_truth: Optional[str] = ""
    ground_truth_label: Optional[str] = ""
    location: str
    phone_model: str
    phone_uid: str
    plot_barcode: str
    plot_end_timestamp: str
    plot_ref: Optional[str] = ""
    plot_start_timestamp: str
    plot_uid: str
    project_name: Optional[str] = ""
    protocol: str
    replication_number: str
    season: str
    site_name: str
    task: str
    trait: str
    trial_name: str
    unitOfDuration: Optional[str] = ""
    uploaded_by_user: str
    collected_by: str = ""
    upload_timestamp: Optional[datetime] = ""

class NumericData(BaseModel):
    plot_uid: str
    plot_ref: Optional[str] = ""
    plot_barcode: str
    data_collection: str
    genotype: str
    replication_number: str
    site_name: str
    trial_name: str
    season: str
    field: str
    location: str
    task: str
    trait: str
    data_type: str
    protocol: str
    date: Optional[str] = ""
    scoring: Optional[str] = ""
    scoring_list: Optional[list] = []
    score_type: Optional[str] = ""
    planting_date: str
    current_plot: str
    total_plot_count: str
    growth_stage: str
    uploaded_by_user: str
    collected_by: str = ""
    environment: str
    phone_uid: str
    upload_timestamp: Optional[datetime] = ""
    durationOfCollection: Optional[str] = ""
    unitOfDuration: Optional[str] = ""
    project_name: Optional[str] = ""

class DateData(BaseModel):
    plot_uid: str
    plot_ref: Optional[str] = ""
    plot_barcode: str
    data_collection: str
    genotype: str
    replication_number: str
    site_name: str
    trial_name: str
    season: str
    field: str
    location: str
    task: str
    trait: str
    data_type: str
    protocol: str
    current_date: str
    planting_date: str
    current_plot: str
    total_plot_count: str
    growth_stage: str
    uploaded_by_user: str
    collected_by: str = ""
    environment: str
    phone_uid: str
    upload_timestamp: Optional[datetime] = ""
    durationOfCollection: Optional[str] = ""
    unitOfDuration: Optional[str] = ""
    project_name: Optional[str] = ""