import json
import datetime
import re
from typing import Dict, Any, List

from telethon.sync import TelegramClient
from telethon.sessions import StringSession
from sqlalchemy.orm import Session
from sqlalchemy import or_, cast, String

from config import settings
from model import Message, engine, ChannelRule, create_tables

# ------------------------ 规则缓存与判断 ------------------------
RULES_CACHE = {}

def load_rules_cache():
    global RULES_CACHE
    try:
        with Session(engine) as session:
            rules = session.query(ChannelRule).filter_by(enabled=True).all()
            RULES_CACHE = {
                r.channel: {
                    'exclude_netdisks': set((r.exclude_netdisks or [])),
                    'exclude_keywords': [kw.lower() for kw in (r.exclude_keywords or []) if kw],
                    'exclude_tags': set((r.exclude_tags or [])),
                }
                for r in rules
            }
            print(f"⚙️ 已加载规则 {len(RULES_CACHE)} 条")
    except Exception as e:
        print(f"⚠️ 加载规则失败: {e}")

def should_drop_by_rules(channel: str, parsed: dict) -> bool:
    if not channel:
        return False
    rule = RULES_CACHE.get(channel)
    if not rule:
        return False
    links = parsed.get('links') or {}
    if links and rule['exclude_netdisks'] and (set(links.keys()) & rule['exclude_netdisks']):
        return True
    kws = rule['exclude_keywords']
    if kws:
        title = (parsed.get('title') or '').lower()
        desc = (parsed.get('description') or '').lower()
        for kw in kws:
            if kw and (kw in title or kw in desc):
                return True
    tags = set(parsed.get('tags') or [])
    if tags and rule['exclude_tags'] and (tags & rule['exclude_tags']):
        return True
    return False

# ------------------------ 文本解析（与 monitor.py 保持一致） ------------------------

def parse_message(text: str) -> Dict[str, Any]:
    lines = text.split('\n')
    title = ''
    description = ''
    links = {}
    tags = []
    source = ''
    channel = ''
    group = ''
    bot = ''
    current_section = None
    desc_lines = []

    netdisk_map = [
        (['quark', '夸克'], '夸克网盘'),
        (['aliyundrive', 'aliyun', '阿里', 'alipan'], '阿里云盘'),
        (['baidu', 'pan.baidu'], '百度网盘'),
        (['115.com', '115网盘', '115pan'], '115网盘'),
        (['cloud.189', '天翼', '189.cn'], '天翼云盘'),
        (['123pan', '123.yun'], '123云盘'),
        (['ucdisk', 'uc网盘', 'ucloud', 'drive.uc.cn'], 'UC网盘'),
        (['xunlei', 'thunder', '迅雷'], '迅雷'),
    ]

    if lines and lines[0].strip():
        if lines[0].startswith('名称：'):
            title = lines[0].replace('名称：', '').strip()
        else:
            title = lines[0].strip()

    for idx, line in enumerate(lines[1:] if title else lines):
        line = line.strip()
        if not line:
            continue
        if line.startswith('🏷 标签：') or line.startswith('标签：'):
            tags.extend([tag.strip('#') for tag in line.replace('🏷 标签：', '').replace('标签：', '').split() if tag.strip('#')])
            continue
        if line.startswith('描述：'):
            current_section = 'description'
            desc_lines.append(line.replace('描述：', '').strip())
        elif line.startswith('链接：'):
            current_section = 'links'
            url = line.replace('链接：', '').strip()
            if not url:
                continue
            found = False
            for keys, name in netdisk_map:
                if any(k in url.lower() for k in keys):
                    links[name] = url
                    found = True
                    break
            if not found:
                links['其他'] = url
        elif line.startswith('🎉 来自：'):
            source = line.replace('🎉 来自：', '').strip()
        elif line.startswith('📢 频道：'):
            channel = line.replace('📢 频道：', '').strip()
        elif line.startswith('👥 群组：'):
            group = line.replace('👥 群组：', '').strip()
        elif line.startswith('🤖 投稿：'):
            bot = line.replace('🤖 投稿：', '').strip()
        elif current_section == 'description':
            desc_lines.append(line)
        else:
            desc_lines.append(line)

    desc_text = '\n'.join(desc_lines)
    pattern = re.compile(r'([\u4e00-\u9fa5A-Za-z0-9#]+)[：:](https?://[^\s]+)')
    matches = pattern.findall(desc_text)
    for key, url in matches:
        found = False
        for keys, name in netdisk_map:
            if any(k in url.lower() or k in key for k in keys):
                links[name] = url
                found = True
                break
        if not found:
            links[key.strip()] = url
    desc_text = pattern.sub('', desc_text)

    url_pattern = re.compile(r'(https?://[^\s]+)')
    for url in url_pattern.findall(desc_text):
        found = False
        for keys, name in netdisk_map:
            if any(k in url.lower() for k in keys):
                links[name] = url
                found = True
                break
        if not found:
            links['其他'] = url
    desc_text = url_pattern.sub('', desc_text)

    tag_pattern = re.compile(r'#([\u4e00-\u9fa5A-Za-z0-9_]+)')
    found_tags = tag_pattern.findall(desc_text)
    if found_tags:
        tags.extend(found_tags)
        desc_text = tag_pattern.sub('', desc_text)

    tags = list(set(tags))
    netdisk_names = ['夸克', '迅雷', '百度', 'UC', '阿里', '天翼', '115', '123云盘']
    netdisk_name_pattern = re.compile(r'(' + '|'.join(netdisk_names) + r')')
    desc_text = netdisk_name_pattern.sub('', desc_text)

    desc_lines_final = [line for line in desc_text.strip().split('\n') if line.strip() and not re.fullmatch(r'[.。·、,，-]+', line.strip())]
    description = '\n'.join(desc_lines_final)

    return {
        'title': title,
        'description': description,
        'links': links,
        'tags': tags,
        'source': source,
        'channel': channel,
        'group_name': group,
        'bot': bot
    }

# ------------------------ 覆盖写入（以链接为唯一），批量优化 ------------------------

def build_existing_link_index(session: Session) -> Dict[str, int]:
    link_to_id: Dict[str, int] = {}
    q = session.query(Message.id, Message.links).filter(Message.links.isnot(None))
    for mid, links in q.yield_per(1000):
        try:
            for url in (links or {}).values():
                if url and isinstance(url, str):
                    link_to_id[url] = mid
        except Exception:
            continue
    return link_to_id

# ------------------------ 导出全部历史到 txt（JSONL） ------------------------

def export_history_txt(output_path: str):
    api_id = settings.TELEGRAM_API_ID
    api_hash = settings.TELEGRAM_API_HASH
    # 优先使用单独的导出会话以避免与线上监控/其他进程冲突
    string_session = None
    if getattr(settings, 'EXPORT_STRING_SESSION', None):
        string_session = settings.EXPORT_STRING_SESSION.strip()
        print('🔐 使用 EXPORT_STRING_SESSION 进行导出')
    elif getattr(settings, 'STRING_SESSION', None):
        string_session = settings.STRING_SESSION.strip()
        print('🔐 使用 STRING_SESSION 进行导出（如遇会话冲突，请改用 EXPORT_STRING_SESSION）')
    if not string_session:
        raise RuntimeError("未配置 EXPORT_STRING_SESSION 或 STRING_SESSION，请在 .env 中设置其中一个后再运行该脚本")

    target_channel = 'bsbdbfjfjff'
    total = 0
    from telethon.errors.rpcerrorlist import AuthKeyDuplicatedError
    with TelegramClient(StringSession(string_session), api_id, api_hash) as client, open(output_path, 'w', encoding='utf-8') as f:
        print(f"📤 正在导出频道 @{target_channel} 的全部历史消息到 {output_path}（JSONL，一行一条）...")
        try:
            for msg in client.iter_messages(target_channel, reverse=True):
                total += 1
                text = getattr(msg, 'message', None) or getattr(msg, 'raw_text', None) or ''
                dt = getattr(msg, 'date', None)
                data = {
                    'id': getattr(msg, 'id', None),
                    'date': (dt.isoformat() if isinstance(dt, datetime.datetime) else None),
                    'text': text,
                }
                f.write(json.dumps(data, ensure_ascii=False) + '\n')
                if total % 200 == 0:
                    print(f"  · 已导出 {total} 条...", flush=True)
        except AuthKeyDuplicatedError:
            print("❌ Telethon 会话在其他设备/进程同时使用。请停止其他正在使用相同会话的任务，或在 .env 中配置 EXPORT_STRING_SESSION 为单独的 StringSession 后重试。", flush=True)
            raise
    print(f"✅ 导出完成，共 {total} 条。")

# ------------------------ 从 txt 批量导入数据库（只导入含网盘链接），链接唯一覆盖 ------------------------

def import_from_txt(input_path: str):
    create_tables()
    load_rules_cache()

    target_channel = 'bsbdbfjfjff'
    inserted = 0
    updated = 0
    skipped_non_netdisk = 0

    with Session(engine) as session:
        link_index = build_existing_link_index(session)
        print(f"🧩 现有链接索引载入完成（{len(link_index)} 条唯一链接）")

        batch_add: List[Message] = []
        batch_ops = 0
        BATCH_SIZE = 200

        with open(input_path, 'r', encoding='utf-8') as f:
            for line_no, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                text = (obj.get('text') or '').strip()
                if not text:
                    continue

                parsed = parse_message(text)
                parsed['channel'] = target_channel
                # 只导入“关于网盘”的消息
                if not parsed.get('links'):
                    skipped_non_netdisk += 1
                    continue
                # 规则过滤
                if should_drop_by_rules(target_channel, parsed):
                    continue

                ts = None
                if obj.get('date'):
                    try:
                        ts = datetime.datetime.fromisoformat(obj['date'])
                    except Exception:
                        pass
                if not ts:
                    ts = datetime.datetime.utcnow()

                urls = set((parsed.get('links') or {}).values())
                target_id = None
                for u in urls:
                    if u in link_index:
                        target_id = link_index[u]
                        break

                if target_id:
                    # 更新路径：加载并覆盖
                    target = session.query(Message).get(target_id)
                    if target is not None:
                        target.timestamp = ts
                        target.title = parsed.get('title')
                        target.description = parsed.get('description')
                        target.links = parsed.get('links')
                        target.tags = parsed.get('tags')
                        target.source = parsed.get('source')
                        target.channel = parsed.get('channel')
                        target.group_name = parsed.get('group_name')
                        target.bot = parsed.get('bot')
                        updated += 1
                        # 更新索引：使用新链接集合指向同一 id
                        for u in urls:
                            link_index[u] = target.id
                    else:
                        # 异常情况：索引存在但找不到记录，走插入
                        m = Message(timestamp=ts, **parsed)
                        batch_add.append(m)
                        for u in urls:
                            link_index[u] = -1  # 占位，commit后更新
                        inserted += 1
                else:
                    # 插入路径
                    m = Message(timestamp=ts, **parsed)
                    batch_add.append(m)
                    for u in urls:
                        link_index[u] = -1
                    inserted += 1

                batch_ops += 1
                if batch_ops >= BATCH_SIZE:
                    session.add_all(batch_add)
                    session.commit()
                    # commit 后，填充新增记录的 id 到索引
                    for m in batch_add:
                        for u in (m.links or {}).values():
                            link_index[u] = m.id
                    batch_add.clear()
                    batch_ops = 0
                    print(f"  · 进度：新增 {inserted}，更新 {updated}，跳过非网盘 {skipped_non_netdisk}", flush=True)

        if batch_add:
            session.add_all(batch_add)
            session.commit()
            for m in batch_add:
                for u in (m.links or {}).values():
                    link_index[u] = m.id
            batch_add.clear()

    print(f"✅ 导入完成：新增 {inserted} 条，覆盖更新 {updated} 条，跳过非网盘 {skipped_non_netdisk} 条")

# ------------------------ 主流程：先导出再导入 ------------------------

def main():
    export_path = 'export_bsbdbfjfjff_all.txt'
    export_history_txt(export_path)
    import_from_txt(export_path)

if __name__ == '__main__':
    main()