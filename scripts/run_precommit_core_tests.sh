#!/bin/bash
set -euo pipefail

# Keep pre-commit on a small smoke set that covers the main runtime path.
CORE_TEST_FILES=(
  tests/test_rss_worker_runtime_config.py
  tests/test_worker_ai_processor_module.py
  tests/test_worker_loop_runner.py
  tests/test_post_processor.py
  tests/test_topic_prompt_v4_worker.py
  tests/test_tag_validate.py
  tests/test_postgres_db.py
  tests/test_postgres_env.py
  tests/test_redis_stream_queue.py
  tests/test_manual_rss_trigger_api.py
  tests/test_alphavault_reflex_app.py
  tests/test_architecture_boundaries.py
  tests/test_cloud_schema_helpers_removed.py
)

PYTEST_CORE_CMD=(uv run pytest -p no:cacheprovider -q "${CORE_TEST_FILES[@]}")
printf -v DB_TEST_CMD '%q ' "${PYTEST_CORE_CMD[@]}"
export DB_TEST_CMD

mise run test:db-hook
