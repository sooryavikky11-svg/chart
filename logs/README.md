# Collection audit logs

Every collection run writes:

- `latest.json`: status of the latest run.
- `runs/<UTC timestamp>.json`: immutable per-run audit record.

The log records success or failure per feed, errors, bar counts, changed files,
and earliest/latest timestamps. A partial feed failure makes the workflow fail
after successfully fetched data and the audit log have been committed.

