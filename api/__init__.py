"""Steam API module for Arma 3 server content management.

This module provides a facade for Steam CDN operations with automatic
retry and session reset capabilities.

Public API:
    - SteamSession: Session class with automatic retry/reset and plan methods
    - ContentSyncer: Syncer class for constructing sync plans
    - SyncPlan: Executable sync plan returned by ContentSyncer.sync()
    - CDLC_IDS: Mapping of CDLC names to depot IDs

Example usage:
    session = SteamSession.login(username, password, config=config)
    
    # Build sync plans (no download yet)
    depot_plan = session.plan_depot_sync(233781)
    workshop_plan = session.plan_workshop_sync(843425103)
    
    # Execute plans to perform downloads
    depot_plan.execute()
    workshop_plan.execute()
"""

from .config import CDLC_IDS, DEFAULT_CONFIG, ARMA3_SERVER_APP_ID
from .session import SteamSession
from .sync import ContentSyncer, SyncPlan

__all__ = [
    "SteamSession",
    "ContentSyncer",
    "SyncPlan",
    "CDLC_IDS",
    "DEFAULT_CONFIG",
    "ARMA3_SERVER_APP_ID",
]
