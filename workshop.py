import os
import re
import subprocess
import urllib.request
import shutil
import threading
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse
from pathlib import Path

import keys
from api.config import resolve_config

USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36"  # noqa: E501

def preset(mod_file, session, config=None):
    """Download mods from a preset HTML file.
    
    Uses parallel manifest fetching and diff construction, followed by
    sequential mod-by-mod downloads.
    
    Args:
        mod_file: Path or URL to the preset HTML file.
        session: SteamSession instance for downloading mods.
        config: Optional config map (uses download_max_workers for parallelism).
        
    Returns:
        List of mod directory paths.
    """
    if mod_file.startswith("http"):
        req = urllib.request.Request(
            mod_file,
            headers={"User-Agent": USER_AGENT},
        )
        remote = urllib.request.urlopen(req)
        with open("preset.html", "wb") as f:
            f.write(remote.read())
        mod_file = "preset.html"
    
    mods = []
    with open(mod_file) as f:
        html = f.read()
        regex = r"filedetails\/\?id=(\d+)\""
        matches = re.finditer(regex, html, re.MULTILINE)
        for _, match in enumerate(matches, start=1):
            mods.append(int(match.group(1)))
    
    resolved_config = resolve_config(config)
    max_workers = resolved_config.get("download_max_workers", 4)
    
    # Thread-safe collection for sync plans
    plans = []
    plans_lock = threading.Lock()
    
    # Thread-safe progress counter
    progress_counter = [0]  # Using list for mutable closure
    progress_lock = threading.Lock()
    total_mods = len(mods)
    
    # Thread-local storage for per-worker sessions
    thread_local = threading.local()
    
    def get_thread_session():
        """Get or create a session for the current worker thread."""
        if not hasattr(thread_local, 'session'):
            thread_local.session = session.clone(connect=True)
        return thread_local.session
    
    def build_plan(workshop_id):
        """Build sync plan for a single workshop item (runs in thread)."""
        thread_session = get_thread_session()
        try:
            plan = thread_session.plan_workshop_sync(workshop_id)
            with plans_lock:
                plans.append((workshop_id, plan))
        except Exception as e:
            with plans_lock:
                plans.append((workshop_id, None))
            with progress_lock:
                print(f"Failed to build sync plan for workshop {workshop_id}: {e}")
        
        # Thread-safe progress update
        with progress_lock:
            progress_counter[0] += 1
            print(f"[{progress_counter[0]}/{total_mods}] Built sync plan for workshop {workshop_id}")
    
    # Phase 1: Parallel manifest fetching and diff construction
    print(f"Building sync plans for {total_mods} mods (max {max_workers} workers)...")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(build_plan, mods)
    
    # Phase 2: Sequential downloads
    moddirs = []
    for workshop_id, plan in plans:
        moddir = "workshop/" + str(workshop_id)
        if plan is None:
            print(f"Skipping workshop {workshop_id} due to plan construction failure.")
            continue
        plan.execute()
        moddirs.append(moddir)
    
    # Copy keys after all downloads
    for moddir in moddirs:
        keys.copy("server/" + moddir)
    
    return moddirs
