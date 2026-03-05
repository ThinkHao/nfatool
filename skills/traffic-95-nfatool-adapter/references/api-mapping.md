# API Mapping

## Metadata / Data Source

- `GET /api/meta/data-sources`
- `GET /api/meta/data-sources/instances?source_type=...`
- `POST /api/meta/data-sources/test`
- `POST /api/meta/data-sources/instances`
- `DELETE /api/meta/data-sources/instances`

## Tasks

- `GET /api/tasks/page`
- `POST /api/tasks`
- `PUT /api/tasks/{id}`
- `DELETE /api/tasks/{id}`
- `POST /api/tasks/{id}/run`
- batch operations supported by existing endpoints

## Jobs / Artifacts

- `GET /api/jobs/page`
- `GET /api/jobs/{job_id}/download?file=...`
- `POST /api/jobs/batch-download/preview`
- `POST /api/jobs/batch-download`

## Update

- `GET /api/meta/update`
- `POST /api/meta/update/apply`
