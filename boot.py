import os
import subprocess
from pathlib import Path

ENV_FILE = os.environ.get("ENV_FILE", "/data/.env")

REQUIRED = [
    "TELEGRAM_API_ID",
    "TELEGRAM_API_HASH",
    "DATABASE_URL",
    "DEFAULT_CHANNELS",
]


def load_env_from_file(path: str):
    p = Path(path)
    if not p.exists():
        return
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            k, v = line.split("=", 1)
            # Do not overwrite already-set envs from platform
            os.environ.setdefault(k.strip(), v.strip())


# 先尝试从 /data/.env 加载（支持先部署，后在网页里配置）
load_env_from_file(ENV_FILE)

missing = [k for k in REQUIRED if not os.environ.get(k)]

if missing:
    # 环境变量不齐：启动引导页（端口 8501）
    cmd = [
        "streamlit", "run", "setup.py", "--server.port", "8501", "--server.address", "0.0.0.0"
    ]
    subprocess.run(cmd, check=False)
else:
    # 环境齐全：根据 RUN_MODE 决定启动
    run_mode = os.environ.get("RUN_MODE", "full").lower()  # full / ui
    if run_mode == "ui":
        # 只启动前台 UI（用于调试或轻量模式）
        subprocess.run([
            "bash", "-lc",
            "streamlit run web.py --server.port 8501 --server.address 0.0.0.0"
        ], check=False)
    else:
        # 启动：初始化 -> 监控 + 前台 + 后台
        subprocess.run([
            "bash", "-lc",
            "python init_db.py && (python monitor.py & streamlit run web.py --server.port 8501 --server.address 0.0.0.0 & streamlit run 后台.py --server.port 8502 --server.address 0.0.0.0 & wait)"
        ], check=False)