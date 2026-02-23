from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, case, select, desc
from sqlalchemy.orm import Session, aliased
from sqlalchemy.sql import over
from app.db.models import Diagnosis, Job, task_runs
from app.db.database import SessionLocal
 
# FastAPI app instance
router  = APIRouter()
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
 
 
# API to get task run statistics
@router.get("/task-runs/stats")
def get_task_run_stats():
    session = SessionLocal()
    try:
        total = session.query(func.count(task_runs.task_run_id)).scalar()

        result = session.query(
            func.sum(case((task_runs.status == 'FAILED', 1), else_=0)).label("failed"),
            func.sum(case((task_runs.status == 'SUCCESS', 1), else_=0)).label("success")
        ).one()

        success = result.success or 0
        failed = result.failed or 0

        return {
            "total_runs": total,
            "success": {
                "count": success,
                "percent": round((success / total) * 100, 2) if total else 0
            },
            "failed": {
                "count": failed,
                "percent": round((failed / total) * 100, 2) if total else 0
            }
        }
    finally:
        session.close()

# API to get pipline status
@router.get("/pipelines/stats")
def get_pipeline_status(db: Session = Depends(get_db)):
    error_count_window = over(func.count(task_runs.error_message), partition_by=task_runs.job_id)

    query = (
        select(
            task_runs.job_id,
            task_runs.status,
            task_runs.start_time,
            task_runs.end_time,
            task_runs.error_message,
            error_count_window.label("error_count")
        )
        .distinct(task_runs.job_id)
        .order_by(task_runs.job_id, task_runs.start_time.desc())
    )

    result = db.execute(query).fetchall()

    # Convert to list of dicts for JSON serialization
    return [
        {
            "job_id": row.job_id,
            "status": row.status,
            "start_time": row.start_time,
            "end_time": row.end_time,
            "error_message": row.error_message,
            "error_count": row.error_count
        }
        for row in result
    ]

@router.get("/error/top-errors")
def get_top_errors(db: Session = Depends(get_db)):
    # Window function: count occurrences of each error_message
    error_count = over(func.count(), partition_by=task_runs.error_message).label("error_count")

    # Base subquery: distinct on error_message, with count and other fields
    subquery = (
        select(
            task_runs.error_message,
            error_count,
            task_runs.job_id,
            task_runs.end_time
        )
        .where(task_runs.error_message != None)
        .distinct(task_runs.error_message)
        .order_by(task_runs.error_message, task_runs.end_time.desc())
        .subquery()
    )
    # Alias for subquery
    sub = aliased(subquery)

    # Outer query: select from subquery, order by error_count desc, limit 10
    query = (
        select(
            sub.c.error_message,
            sub.c.error_count,
            sub.c.job_id,
            sub.c.end_time
        )
        .order_by(desc(sub.c.error_count))
        .limit(10)
    )

    result = db.execute(query).fetchall()

    # Convert to JSON serializable list of dicts
    return [
        {
            "error_message": row.error_message,
            "error_count": row.error_count,
            "job_id": row.job_id,
            "end_time": row.end_time
        }
        for row in result
    ]

@router.get("/runs-list/job-runs")
def get_job_runs(db: Session = Depends(get_db)):
    # 1 if any row in the run has FAILED, else 0
    failed_flag = func.max(case((task_runs.status == 'FAILED', 1), else_=0))
    agg_status = case((failed_flag == 1, 'FAILED'), else_='SUCCESS').label("status")

    query = (
        select(
            task_runs.run_id.label("run_id"),
            agg_status,
            # assuming these are constant within a run; pick one via aggregate
            func.max(Job.name).label("job_name"),
            func.max(task_runs.run_page_url).label("run_page_url"),
            func.min(task_runs.start_time).label("start_time"),
            func.max(task_runs.end_time).label("end_time"),
        )
        .join(Job, task_runs.job_id == Job.job_id)
        .group_by(task_runs.run_id)
        .order_by(desc(func.max(task_runs.end_time)))
    )

    rows = db.execute(query).all()

    return [
        {
            "run_id": r.run_id,
            "status": r.status,
            "job_name": r.job_name,
            "run_page_url": r.run_page_url,
            "start_time": r.start_time,
            "end_time": r.end_time,
        }
        for r in rows
    ]

# e.g. GET /runs-list/job-runs/12345/tasks
@router.get("/runs-list/job-runs/{run_id}/tasks")
def get_job_run_rows(run_id: str, db: Session = Depends(get_db)):
    q = (
        select(
            task_runs.task_run_id,              # <-- added
            Job.name.label("job_name"),
            task_runs.run_id,
            task_runs.task_name,
            task_runs.start_time,
            task_runs.end_time,
            task_runs.status,
            task_runs.notebook_path,
        )
        .join(Job, task_runs.job_id == Job.job_id)
        .where(task_runs.run_id == run_id)
        .order_by(task_runs.start_time.asc(), task_runs.task_run_id.asc())
    )

    rows = db.execute(q).all()
    return [
        {
            "task_run_id": r.task_run_id,
            "job_name": r.job_name,
            "run_id": r.run_id,
            "task_name": r.task_name,
            "start_time": r.start_time,
            "end_time": r.end_time,
            "status": r.status,
            "notebook_path": r.notebook_path,
        }
        for r in rows
    ]


@router.get("/runs-list/job-runs/{run_id}/tasks/{task_run_id}")
def get_task_run_detail(run_id: str, task_run_id: str, db: Session = Depends(get_db)):
    q = (
        select(
            task_runs,                       # entire task_runs entity
            Job.name.label("job_name"), # job name
            Diagnosis                        # entire diagnosis entity (if any)
        )
        .join(Job, task_runs.job_id == Job.job_id)
        .outerjoin(Diagnosis, Diagnosis.task_run_id == task_runs.task_run_id)
        .where(
            task_runs.run_id == run_id,
            task_runs.task_run_id == task_run_id,
        )
        .limit(1)
    )

    row = db.execute(q).first()
    if not row:
        raise HTTPException(status_code=404, detail="Task run not found")

    task_entity, name, diag_entity = row

    task_dict = {c.name: getattr(task_entity, c.name) for c in task_runs.__table__.columns}
    diag_dict = None
    if diag_entity is not None:
        diag_dict = {c.name: getattr(diag_entity, c.name) for c in Diagnosis.__table__.columns}

    return {
        "job_name": name,
        "task_run": task_dict,
        "diagnosis": diag_dict,
    }




@router.get("/alerts")
def get_failed_alerts(db: Session = Depends(get_db)):
    q = (
        select(
            # task_runs
            task_runs.task_run_id.label("task_run_id"),
            task_runs.run_id.label("run_id"),
            task_runs.job_id.label("job_id"),
            task_runs.status.label("status"),
            task_runs.run_page_url.label("run_page_url"),
            # Diagnosis (all as text)
            Diagnosis.error_type.label("error_type"),
            Diagnosis.severity_score.label("severity_score"),
            Diagnosis.confidence.label("confidence"),
            Diagnosis.reason.label("reason"),
            # Job
            Job.name.label("job_name"),
        )
        .select_from(task_runs)
        .join(Diagnosis, Diagnosis.task_run_id == task_runs.task_run_id)
        .join(Job, Job.job_id == task_runs.job_id)
        .where(task_runs.status == "FAILED")
        .order_by(task_runs.start_time.desc())
    )

    rows = db.execute(q).mappings().all()  # returns list of RowMapping objects
    return [dict(row) for row in rows]

