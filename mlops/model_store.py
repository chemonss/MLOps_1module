import json
import os
import tempfile
from pathlib import Path
from catboost import CatBoostClassifier
from mlops.config import MODEL_DIR

def current_version():
    pointer = MODEL_DIR / "current.json"
    if not pointer.exists():
        return None
    return json.loads(pointer.read_text())["version"]

def load_current():
    version = current_version()
    if version is None:
        return None, None
    model = CatBoostClassifier()
    model.load_model(str(MODEL_DIR / f"{version}.cbm"))
    return version, model

def publish(candidate_file, version):
    """Publish artifact first; switch pointer atomically only after validation."""
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    target = MODEL_DIR / f"{version}.cbm"
    os.replace(candidate_file, target)
    fd, tmp = tempfile.mkstemp(dir=MODEL_DIR, prefix=".current-")
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump({"version": version}, stream)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, MODEL_DIR / "current.json")
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)
