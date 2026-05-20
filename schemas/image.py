from pydantic import BaseModel
from datetime import datetime
from typing import Optional

class Image(BaseModel):
    image_uuid: str
    image_name: str
    bucket_prefix: Optional[str] = ""
    project_name: Optional[str] = ""
    gcs_img_path: Optional[str] = ""
    gcs_cam_metadata_path: Optional[str] = ""
    data_collection: str
    plot_uid: str
    plot_ref: Optional[str] = ""
    plot_barcode: str
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
    date: str
    gcs_dir_path: Optional[str] = ""
    ground_truth_label: Optional[str] = ""
    ground_truth: Optional[str] = ""
    growth_stage: str
    uploaded_by_user: str
    collected_by: str = ""
    environment: str
    phone_uid: str
    annotated_img_path: Optional[str] = ""
    results: Optional[dict] = {}
    upload_timestamp: Optional[datetime] = ""

class ImageData(BaseModel):
    metadata: dict
    image_name: str
    gcs_path: Optional[str] = ""
    image_uuid: str

class SignedURLResponse(BaseModel):
    metadata: Image
    image_name: str
    gcs_path: str
    signed_url: str
    camera_metadata_signed_url: str
    image_uuid: str
    environment: Optional[str] = ""

class TwoImage(BaseModel):
    image_uuid_top: str
    image_uuid_bottom: str
    image_name_top: str
    image_name_bottom: str
    gcs_img_path_top: Optional[str] = ""
    gcs_img_path_bottom: Optional[str] = ""
    gcs_cam_metadata_path_top: Optional[str] = ""
    gcs_cam_metadata_path_bottom: Optional[str] = ""
    bucket_prefix: Optional[str] = ""
    project_name: Optional[str] = ""
    data_collection: str
    plot_uid: str
    plot_ref: Optional[str] = ""
    plot_barcode: str
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
    date: str
    gcs_dir_path: Optional[str] = ""
    ground_truth_label: Optional[str] = ""
    ground_truth: Optional[str] = ""
    growth_stage: str
    uploaded_by_user: str
    collected_by: str = ""
    environment: str
    phone_uid: str
    annotated_img_path: Optional[str] = ""
    results: Optional[dict] = {}
    upload_timestamp: Optional[datetime] = ""

class TwoImageData(BaseModel):
    metadata: dict
    image_name_top: str
    image_name_bottom: str
    gcs_path: Optional[str] = ""
    image_uuid_top: str
    image_uuid_bottom: str

class TwoSignedURLResponse(BaseModel):
    metadata: TwoImage
    image_name_top: str
    image_name_bottom: str
    gcs_path: str
    signed_url_top: str
    camera_metadata_signed_url_top: str 
    signed_url_bottom: str
    camera_metadata_signed_url_bottom: str 
    image_uuid_top: str
    image_uuid_bottom: str
    environment: Optional[str] = ""