"""Content synchronization using manifest diffs and state tracking."""

import os
import time

from .config import STATE_VERSION, COMBINATION_METHOD, resolve_config
from .state import (
    StateManager, 
    normalize_path, 
    content_hash_hex, 
    compute_file_hash, 
    combined_mod_hash,
    hash_file_content,
)
from .download import Downloader


def build_remote_state(destination_root, files):
    """Build remote manifest state for downstream diffing.
    
    Args:
        destination_root: Local root where files should land.
        files: Iterable of CDN file objects.
        
    Returns:
        Tuple of (state_dict, file_map).
    """
    entries = []
    file_map = {}

    for file_obj in files:
        local_path = normalize_path(os.path.join(destination_root, file_obj.filename))
        content_hash = content_hash_hex(file_obj)
        file_hash = compute_file_hash(local_path, content_hash)
        entry = {
            "path": local_path,
            "file_hash": file_hash,
            "content_hash": content_hash,
            "size": getattr(file_obj, "size", 0),
            "file": file_obj,
        }
        entries.append(entry)
        file_map[local_path] = entry

    combined = combined_mod_hash([entry["file_hash"] for entry in entries])
    return {"combined_hash": combined, "files": entries}, file_map


def diff_states(remote_state, local_state):
    """Compare remote vs local state into download/delete/unchanged buckets.
    
    Args:
        remote_state: State dict from build_remote_state.
        local_state: State dict from StateManager.load_state or None.
        
    Returns:
        Tuple of (to_download, to_delete, unchanged) lists.
    """
    remote_map = {entry["path"]: entry for entry in remote_state.get("files", [])}
    local_files = local_state.get("files", []) if local_state else []
    local_map = {entry["path"]: entry for entry in local_files}

    to_download = []
    to_delete = []
    unchanged = []

    for path, remote_entry in remote_map.items():
        local_entry = local_map.get(path)
        if not local_entry or local_entry.get("file_hash") != remote_entry["file_hash"]:
            to_download.append(remote_entry)
        else:
            unchanged.append(remote_entry)

    for path, local_entry in local_map.items():
        if path not in remote_map:
            to_delete.append(local_entry)

    return to_download, to_delete, unchanged


def remove_local_files(entries):
    """Delete local files listed in entries, ignoring errors.
    
    Args:
        entries: List of file entry dicts with 'path' key.
    """
    for entry in entries:
        local_path = os.path.normpath(entry["path"])
        try:
            if os.path.exists(local_path):
                os.remove(local_path)
        except OSError:
            print(f"Warning: failed to delete {local_path}")


class SyncPlan:
    """Represents a computed sync diff that can be executed later.
    
    Enables separating manifest fetching/diff computation from actual downloads,
    allowing concurrent diff construction followed by sequential execution.
    """

    def __init__(
        self,
        state_manager,
        item_id,
        label,
        destination,
        config,
        remote_state,
        to_download,
        to_delete,
        unchanged,
        up_to_date=False,
        empty=False,
    ):
        """Initialize a sync plan with diff results.
        
        Args:
            state_manager: StateManager instance for persisting state.
            item_id: Depot or workshop ID.
            label: Human-readable label for logging.
            destination: Local root where files should land.
            config: Resolved config map.
            remote_state: Remote state dict from build_remote_state.
            to_download: List of entries to download.
            to_delete: List of entries to delete locally.
            unchanged: List of unchanged entries.
            up_to_date: True if no sync is needed.
            empty: True if manifest has no files.
        """
        self._state_manager = state_manager
        self._item_id = item_id
        self._label = label
        self._destination = destination
        self._config = config
        self._remote_state = remote_state
        self._to_download = to_download
        self._to_delete = to_delete
        self._unchanged = unchanged
        self._up_to_date = up_to_date
        self._empty = empty
        self._executed = False

    @property
    def up_to_date(self):
        """True if content is already up-to-date, no downloads needed."""
        return self._up_to_date

    @property
    def empty(self):
        """True if manifest has no files."""
        return self._empty

    @property
    def needs_download(self):
        """True if there are files to download."""
        return bool(self._to_download) and not self._up_to_date and not self._empty

    @property
    def download_count(self):
        """Number of files to download."""
        return len(self._to_download) if self._to_download else 0

    @property
    def delete_count(self):
        """Number of files to delete."""
        return len(self._to_delete) if self._to_delete else 0

    @property
    def unchanged_count(self):
        """Number of unchanged files."""
        return len(self._unchanged) if self._unchanged else 0

    @property
    def total_count(self):
        """Total number of files in remote manifest."""
        if self._remote_state:
            return len(self._remote_state.get("files", []))
        return 0

    @property
    def label(self):
        """Human-readable label for this sync plan."""
        return self._label

    @property
    def item_id(self):
        """Depot or workshop ID."""
        return self._item_id

    def execute(self):
        """Execute the sync plan: delete obsolete files, download new/changed files, persist state.
        
        Returns:
            True if execution completed, False if skipped (up-to-date/empty/already executed).
        """
        if self._executed:
            print(f"{self._label}: already executed, skipping.")
            return False

        self._executed = True

        if self._empty:
            print(f"{self._label} has no files in manifest.")
            return False

        if self._up_to_date:
            short_hash = self._remote_state["combined_hash"][:7]
            print(f"{self._label} is up-to-date (version {short_hash}).")
            return False

        print(f"{self._label}: {self.total_count} files in manifest.")
        print(f"  Unchanged: {self.unchanged_count} | To download/update: {self.download_count} | To delete: {self.delete_count}")

        remove_local_files(self._to_delete)
        remove_local_files(self._to_download)

        entry_map = {entry["path"]: entry for entry in self._to_download}
        verified_entries = {}  # path -> checkpoint dict for files verified this run

        if self._to_download:
            def _checkpoint(file_obj):
                path = normalize_path(file_obj.local)
                entry = entry_map.get(path)
                if not entry:
                    return
                try:
                    actual_hash = hash_file_content(file_obj.local)
                except OSError:
                    print(f"Warning: could not read {file_obj.filename} for hash verification, skipping checkpoint")
                    return
                if actual_hash != entry["content_hash"]:
                    print(f"Warning: hash mismatch for {file_obj.filename}: expected {entry['content_hash']}, got {actual_hash}")
                    try:
                        os.remove(file_obj.local)
                    except OSError:
                        pass
                    return
                checkpoint = {
                    "path": entry["path"],
                    "file_hash": entry["file_hash"],
                    "content_hash": entry["content_hash"],
                    "size": entry.get("size", 0),
                    "downloaded_at": time.time(),
                }
                self._state_manager.write_file_entry(self._item_id, checkpoint)
                verified_entries[path] = checkpoint

            downloader = Downloader(config=self._config)
            download_success = downloader.download_files(
                [entry["file"] for entry in self._to_download],
                destination=self._destination,
                post_download_hook=_checkpoint,
            )
            if not download_success or len(verified_entries) < len(self._to_download):
                failed_count = len(self._to_download) - len(verified_entries)
                print(f"{self._label}: {failed_count} file(s) failed to download or failed hash verification.")

        updated_state = self._state_manager.load_state(self._item_id) or {}
        local_map = {entry["path"]: entry for entry in updated_state.get("files", [])}
        now = time.time()
        persisted_files = []

        for entry in self._remote_state["files"]:
            path = entry["path"]
            if path in verified_entries:
                persisted_files.append(verified_entries[path])
            else:
                previous = local_map.get(path)
                if previous and previous.get("file_hash") == entry["file_hash"]:
                    persisted_files.append({
                        "path": path,
                        "file_hash": entry["file_hash"],
                        "content_hash": entry["content_hash"],
                        "size": entry.get("size", 0),
                        "downloaded_at": previous.get("downloaded_at", now),
                    })
                # else: download failed or hash mismatch — omit from state

        actual_combined = combined_mod_hash([e["file_hash"] for e in persisted_files])
        self._state_manager.save_state(self._item_id, actual_combined, persisted_files)
        if actual_combined == self._remote_state["combined_hash"]:
            print(f"{self._label} stored (version {actual_combined[:7]}).")
        else:
            print(f"{self._label} partially stored ({len(persisted_files)}/{len(self._remote_state['files'])} files, version {actual_combined[:7]}).")
        return True


class ContentSyncer:
    """Handles incremental content synchronization."""

    def __init__(self, index_root, config=None):
        """Initialize syncer with index root and config.
        
        Args:
            index_root: Root folder for index state.
            config: Optional config map.
        """
        self.state_manager = StateManager(index_root)
        self._config = resolve_config(config)

    def skip(self, item_id, destination, label):
        """Return an up-to-date plan from local state without touching the CDN.

        Used when skip_sync is set to bypass all manifest fetching.

        Args:
            item_id: Depot or workshop ID.
            destination: Local root where files land.
            label: Human-readable label for logging.

        Returns:
            SyncPlan with up_to_date=True built from local state.
        """
        local_state = self.state_manager.load_state(item_id)
        files = local_state.get("files", []) if local_state else []
        combined = local_state.get("combined_hash", "skipped") if local_state else "skipped"
        print(f"{label}: skip_sync is set, assuming local state is up-to-date ({len(files)} file(s)).")
        return SyncPlan(
            state_manager=self.state_manager,
            item_id=item_id,
            label=label,
            destination=destination,
            config=self._config,
            remote_state={"combined_hash": combined, "files": files},
            to_download=[],
            to_delete=[],
            unchanged=files,
            up_to_date=True,
        )

    def sync(self, files, destination, item_id, label):
        """Construct a sync plan from manifest diffs without executing downloads.
        
        Compares remote manifest against local cached state and returns a SyncPlan
        that can be executed later. This enables concurrent diff construction
        across multiple items followed by sequential download execution.

        Args:
            files: Iterable of CDN file objects from the manifest.
            destination: Local root where files should land.
            item_id: Depot or workshop ID used for index names.
            label: Human-readable label for logging.
            
        Returns:
            SyncPlan object that can be executed via plan.execute().
        """
        if not files:
            return SyncPlan(
                state_manager=self.state_manager,
                item_id=item_id,
                label=label,
                destination=destination,
                config=self._config,
                remote_state=None,
                to_download=[],
                to_delete=[],
                unchanged=[],
                empty=True,
            )

        remote_state, _ = build_remote_state(destination, files)
        local_state = self.state_manager.load_state(item_id)

        self.state_manager.ensure_state_header(
            item_id, 
            combined_hash=local_state.get("combined_hash") if local_state else "pending"
        )

        if local_state and (local_state.get("version") != STATE_VERSION or 
                           local_state.get("method") != COMBINATION_METHOD):
            print(f"{label} index format changed, ignoring cached state.")
            local_state = None

        if (local_state and local_state.get("combined_hash") == remote_state["combined_hash"] and not self._config.get("validate_local")):
            return SyncPlan(
                state_manager=self.state_manager,
                item_id=item_id,
                label=label,
                destination=destination,
                config=self._config,
                remote_state=remote_state,
                to_download=[],
                to_delete=[],
                unchanged=remote_state.get("files", []),
                up_to_date=True,
            )

        to_download, to_delete, unchanged = diff_states(remote_state, local_state)

        if self._config.get("validate_local") and unchanged:
            print(f"{label}: validating {len(unchanged)} local file(s) against disk...")
            redownload = []
            still_unchanged = []
            for entry in unchanged:
                local_path = os.path.normpath(entry["path"])
                try:
                    actual_hash = hash_file_content(local_path)
                    if actual_hash != entry["content_hash"]:
                        print(f"  Validation failed (hash mismatch): {entry['path']}")
                        redownload.append(entry)
                    else:
                        still_unchanged.append(entry)
                except OSError:
                    print(f"  Validation failed (missing): {entry['path']}")
                    redownload.append(entry)
            if redownload:
                print(f"{label}: {len(redownload)} file(s) failed validation, queued for re-download.")
                to_download = to_download + redownload
            unchanged = still_unchanged

        return SyncPlan(
            state_manager=self.state_manager,
            item_id=item_id,
            label=label,
            destination=destination,
            config=self._config,
            remote_state=remote_state,
            to_download=to_download,
            to_delete=to_delete,
            unchanged=unchanged,
            up_to_date=(not to_download and not to_delete and unchanged)
        )
