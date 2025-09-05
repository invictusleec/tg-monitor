import os
import subprocess

REQUIRED = [
    "TELEGRAM_API_ID",
    "TELEGRAM_API_HASH",
    "DATABASE_URL",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "POSTGRES_DB",
    "DEFAULT_CHANNELS",
]

missing = [k for k in REQUIRED if not os.environ.get(k)]

if missing:
    # 环境变量不齐：启动引导页（端口 8501）
    cmd = [
        "streamlit", "run", "setup.py", "--server.port", "8501", "--server.address", "0.0.0.0"
    ]
    subprocess.run(cmd, check=False)
else:
    # 环境齐全：初始化并并行启动监控、前台（可选后台）
    args = os.environ.get("RUN_MODE", "full")  # full / ui
    if args == "ui":
        subprocess.run([
            "bash", "-lc",
            "streamlit run web.py --server.port 8501 --server.address 0.0.0.0"
        ], check=False)
    else:
        subprocess.run([
            "bash", "-lc",
            "python init_db.py && (python monitor.py & streamlit run web.py --server.port 8501 --server.address 0.0.0.0 & streamlit run 后台.py --server.port 8502 --server.address 0.0.0.0 & wait)"
        ], check=False)