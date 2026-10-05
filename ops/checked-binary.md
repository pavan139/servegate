# Checked gateway binary

The [machine-readable verification report](../reports/verification.json) is the authority for the executed test count, skipped tests, installation checks and source digest. The report is refreshed after a layout or source change; use its recorded commands when comparing runs.

The behavioral details are in [race-checks.md](race-checks.md), and measured workload results are in [saturation-runs.md](saturation-runs.md). [admission-and-shutdown.md](admission-and-shutdown.md) states the assumptions those checks exercise. A passing local check records that run; the GitHub workflow has its own status after publishing.
