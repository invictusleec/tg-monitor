import os
from telethon.sync import TelegramClient
from telethon.sessions import StringSession
from config import settings

"""
交互式生成一个全新的 StringSession 并自动写入当前目录下的 .env 文件：
- 使用 config.settings 中的 TELEGRAM_API_ID / TELEGRAM_API_HASH
- 生成后写入/更新 EXPORT_STRING_SESSION=...
注意：此脚本会在控制台提示你输入手机号和验证码。
"""

def write_env_kv(env_path: str, key: str, value: str):
    lines = []
    if os.path.exists(env_path):
        with open(env_path, 'r', encoding='utf-8') as f:
            lines = f.read().splitlines()
    updated = False
    new_lines = []
    for line in lines:
        if line.startswith(f'{key}='):
            new_lines.append(f'{key}={value}')
            updated = True
        else:
            new_lines.append(line)
    if not updated:
        new_lines.append(f'{key}={value}')
    with open(env_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(new_lines) + '\n')


def main():
    api_id = settings.TELEGRAM_API_ID
    api_hash = settings.TELEGRAM_API_HASH
    if not api_id or not api_hash:
        raise RuntimeError('请在 .env 中配置 TELEGRAM_API_ID 和 TELEGRAM_API_HASH')

    print('将创建一个全新的导出会话（EXPORT_STRING_SESSION）。')
    print('请按提示输入手机号（含国家区号，如 +86xxxxxxxxxxx）与验证码。')

    with TelegramClient(StringSession(), api_id, api_hash) as client:
        client.start()  # 交互式登录
        s = client.session.save()

    env_path = os.path.join(os.path.dirname(__file__), '.env')
    write_env_kv(env_path, 'EXPORT_STRING_SESSION', s)
    print('已生成并写入 .env 中的 EXPORT_STRING_SESSION（不会在控制台显示具体值）。')
    print('现在可以重新执行导出脚本。')


if __name__ == '__main__':
    main()