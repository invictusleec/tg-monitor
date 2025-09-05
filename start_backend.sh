#!/usr/bin/env bash
set -euo pipefail

# 启动后台管理（可选）
streamlit run 后台.py --server.port 8502 --server.address 0.0.0.0