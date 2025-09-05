import os
from pathlib import Path
import streamlit as st

ENV_PATH = Path(os.environ.get("ENV_FILE", "/data/.env"))

REQUIRED = [
    "TELEGRAM_API_ID",
    "TELEGRAM_API_HASH",
    "DATABASE_URL",
    "DEFAULT_CHANNELS",
]

OPTIONAL = [
    "STRING_SESSION",
    "RUN_MODE",  # full / ui
]

st.set_page_config(page_title="tg-monitor • 安装向导", page_icon="🛠", layout="centered")
st.title("🛠 首次部署安装向导")

# 读取已有配置作为默认值
existing = {}
if ENV_PATH.exists():
    for line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        existing[k.strip()] = v.strip()

with st.form("setup-form", clear_on_submit=False):
    col1, col2 = st.columns(2)
    with col1:
        TELEGRAM_API_ID = st.text_input("TELEGRAM_API_ID", value=existing.get("TELEGRAM_API_ID", ""))
        TELEGRAM_API_HASH = st.text_input("TELEGRAM_API_HASH", value=existing.get("TELEGRAM_API_HASH", ""))
    with col2:
        DEFAULT_CHANNELS = st.text_input("DEFAULT_CHANNELS (逗号分隔)", value=existing.get("DEFAULT_CHANNELS", "BaiduCloudDisk,tianyirigeng,Aliyun_4K_Movies"))
        RUN_MODE = st.selectbox("RUN_MODE", ["full", "ui"], index=0 if existing.get("RUN_MODE", "full") == "full" else 1)

    st.divider()
    st.markdown("数据库连接串（示例：postgresql://user:password@host:5432/dbname）")
    DATABASE_URL = st.text_input("DATABASE_URL", value=existing.get("DATABASE_URL", ""), placeholder="postgresql://user:password@host:5432/dbname")

    st.divider()
    STRING_SESSION = st.text_area("STRING_SESSION (可选)", value=existing.get("STRING_SESSION", ""), height=120, help="用于以用户身份访问 Telegram")

    submitted = st.form_submit_button("保存配置并重启容器", type="primary")

if submitted:
    errors = []
    if not TELEGRAM_API_ID: errors.append("TELEGRAM_API_ID")
    if not TELEGRAM_API_HASH: errors.append("TELEGRAM_API_HASH")
    if not DATABASE_URL: errors.append("DATABASE_URL")
    if not DEFAULT_CHANNELS: errors.append("DEFAULT_CHANNELS")

    if errors:
        st.error("以下必填项未填写：" + ", ".join(errors))
    else:
        ENV_PATH.parent.mkdir(parents=True, exist_ok=True)
        lines = [
            f"TELEGRAM_API_ID={TELEGRAM_API_ID}",
            f"TELEGRAM_API_HASH={TELEGRAM_API_HASH}",
            f"DATABASE_URL={DATABASE_URL}",
            f"DEFAULT_CHANNELS={DEFAULT_CHANNELS}",
            f"RUN_MODE={RUN_MODE}",
        ]
        if STRING_SESSION:
            lines.append(f"STRING_SESSION={STRING_SESSION}")
        ENV_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")

        st.success("已保存到 /data/.env，容器即将重启以应用配置……")
        st.info("如未自动重启，请手动在平台中重启该应用。")
        # 尝试触发容器重启：直接退出当前进程
        import os as _os, time as _time
        _time.sleep(1.2)
        _os._exit(0)

# 显示当前缺失项提示
missing = [k for k in REQUIRED if not os.environ.get(k, existing.get(k))]
if missing:
    st.warning("当前仍缺少：" + ", ".join(missing))

st.caption("此页面仅在未完成配置时显示；保存并重启后会自动进入正式应用。")