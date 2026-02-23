from sqlalchemy import Column, JSON, Text, Boolean, Integer, BigInteger, TIMESTAMP, ForeignKey, Interval, Double, UUID
from sqlalchemy.ext.declarative import declarative_base
from app.db.database import Base

Base = declarative_base()

class Catalogs(Base):
    __tablename__ = "catalogs"

    name = Column(Text)
    owner = Column(Text)
    catalog_type = Column(Text)
    connection_name = Column(Text)
    metastore_id = Column(Text)
    created_at = Column(TIMESTAMP)
    created_by = Column(Text)
    updated_at = Column(TIMESTAMP)
    updated_by = Column(Text)
    isolation_mode = Column(Text)
    accessible_in_current_workspace = Column(Boolean)
    browse_only = Column(Boolean)
    id = Column(Text, primary_key=True)
    full_name = Column(Text)
    securable_type = Column(Text)
    securable_kind = Column(Text)
    resource_name = Column(Text)
    metastore_version = Column(Text)
    query_federation_attributes_connection_id = Column(Text)
    query_federation_attributes_last_refreshed_timestamp = Column(TIMESTAMP)
    storage_root = Column(Text)
    enable_auto_maintenance = Column(Boolean)
    enable_predictive_optimization = Column(Boolean)
    storage_location = Column(Text)
    effective_auto_maintenance_flag_value = Column(Boolean)
    effective_auto_maintenance_flag_inherited_from_type = Column(Text)
    effective_auto_maintenance_flag_inherited_from_name = Column(Text)
    effective_predictive_optimization_flag_value = Column(Boolean)
    effective_predictive_optimization_flag_inherited_from_type = Column(Text)
    effective_predictive_optimization_flag_inherited_from_name = Column(Text)
    comment = Column(Text)
    delta_sharing_valid_through_timestamp = Column(TIMESTAMP)

class Pipeline(Base):
    __tablename__ = "pipeline"

    pipeline_id = Column(UUID, primary_key=True)
    update_id = Column(UUID)
    cluster_id = Column(Text)
    name = Column(Text)
    state = Column(Text)
    cause = Column(Text)
    start_time = Column(TIMESTAMP)
    end_time = Column(TIMESTAMP)
    full_refresh = Column(Boolean)
    development = Column(Boolean)
    continuous = Column(Boolean)
    channel = Column(Text)
    edition = Column(Text)
    catalog = Column(Text)
    schema_name = Column(Text)
    serverless = Column(Boolean)
    notebook_path = Column(Text)
    created_by = Column(Text)


class Job(Base):
    __tablename__ = "job"

    job_id = Column(BigInteger, primary_key=True)
    
    creator = Column(Text)

    name = Column(Text)
    description = Column(Text)
    created_time = Column(Text)



class Cluster(Base):
    __tablename__ = "cluster"

    cluster_id = Column(Text, primary_key=True)
    pipeline_id = Column(UUID, ForeignKey("pipeline.pipeline_id"))
    cluster_name = Column(Text)
    creator_user_name = Column(Text)
    spark_version = Column(Text)
    state = Column(Text)
    state_message = Column(Text)
    num_workers = Column(Integer)
    node_type_id = Column(Text)
    driver_node_type_id = Column(Text)
    autotermination_minutes = Column(Integer)
    start_time = Column(TIMESTAMP)
    terminated_time = Column(TIMESTAMP)
    termination_type = Column(Text)
    termination_code = Column(Text)
    availability = Column(Text)
    runtime_engine = Column(Text)


class task_runs(Base):
    __tablename__ = "task_runs"

    task_run_id = Column(Text, primary_key=True)
    run_id = Column(Text)
    job_id = Column(BigInteger, ForeignKey("job.job_id"))
    task_name = Column(Text)
    start_time = Column(TIMESTAMP)
    end_time = Column(TIMESTAMP)
    execution_duration = Column(Interval)
    status = Column(Text)
    error_message = Column(Text)
    life_cycle_state = Column(Text)
    result_state = Column(Text)
    user_cancelled_or_timeout = Column(Text)
    cluster_id = Column(Text, ForeignKey("cluster.cluster_id"))
    trigger = Column(Text)
    notebook_path = Column(Text)
    run_type = Column(Text)
    run_page_url = Column(Text)

class Diagnosis(Base):
    __tablename__ = "diagnosis"

    task_run_id = Column(Text, ForeignKey("task_runs.task_run_id"), primary_key=True)
    error_type = Column(Text)
    confidence = Column(Text)
    reason = Column(Text)
    diagnosis = Column(Text)
    severity_score = Column(Text)

class Schema(Base):
    __tablename__ = 'schemas'

    name = Column(Text)
    catalog_name = Column(Text)
    owner = Column(Text)
    enable_auto_maintenance = Column(Boolean)
    enable_predictive_optimization = Column(Boolean)
    metastore_id = Column(Text)
    full_name = Column(Text)
    created_at = Column(TIMESTAMP)
    created_by = Column(Text)
    updated_at = Column(TIMESTAMP)
    updated_by = Column(Text)
    catalog_type = Column(Text)
    schema_id = Column(Text, primary_key=True)
    securable_type = Column(Text)
    securable_kind = Column(Text)
    browse_only = Column(Boolean)
    metastore_version = Column(Text)
    effective_auto_maintenance_flag_value = Column(Boolean)
    effective_predictive_optimization_flag_value = Column(Boolean)
    options_database = Column(Text)
    comment = Column(Text)
    effective_auto_maintenance_flag_inherited_from_type = Column(Text)
    effective_auto_maintenance_flag_inherited_from_name = Column(Text)
    effective_predictive_optimization_flag_inherited_from_type = Column(Text)
    effective_predictive_optimization_flag_inherited_from_name = Column(Text)
    storage_root = Column(Text)
    storage_location = Column(Text)
    properties_owner = Column(Text)
    delta_sharing_valid_through_timestamp = Column(TIMESTAMP)
    provider_id = Column(Text)
    share_name = Column(Text)

class Table(Base):
    __tablename__ = 'tables'

    name = Column(Text)
    catalog_name = Column(Text)
    schema_name = Column(Text)
    table_type = Column(Text)
    data_source_format = Column(Text)
    owner = Column(Text)
    securable_kind = Column(Text)
    generation = Column(Integer)
    metastore_id = Column(Text)
    full_name = Column(Text)
    data_access_configuration_id = Column(Text)
    created_at = Column(TIMESTAMP)
    created_by = Column(Text)
    updated_at = Column(TIMESTAMP)
    updated_by = Column(Text)
    table_id = Column(Text, primary_key=True)
    securable_type = Column(Text)
    browse_only = Column(Boolean)
    metastore_version = Column(Text)
    schema_id = Column(Text)
    catalog_id = Column(Text)
    columns = Column(JSON) 