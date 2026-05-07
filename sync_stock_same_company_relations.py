from __future__ import annotations

import argparse

from alphavault.logging_config import (
    add_log_level_argument,
    configure_logging,
    get_logger,
)
from alphavault.research_workbench import (
    STOCK_SIBLING_SYNC_SOURCE_SECURITY_MASTER,
    get_research_workbench_engine_from_env,
    sync_stock_sibling_relations_from_security_master,
)


logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync stock same-company relations from security_master"
    )
    add_log_level_argument(parser)
    return parser.parse_args()


def sync_stock_same_company_relations() -> int:
    engine = get_research_workbench_engine_from_env()
    relation_count = sync_stock_sibling_relations_from_security_master(
        engine,
        source=STOCK_SIBLING_SYNC_SOURCE_SECURITY_MASTER,
    )
    logger.info("synced stock same-company relations: %s", relation_count)
    logger.info("restart running Reflex or MCP processes to refresh cached rows")
    return relation_count


def main() -> int:
    args = parse_args()
    configure_logging(level=getattr(args, "log_level", ""))
    sync_stock_same_company_relations()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
