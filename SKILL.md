---
name: xiaohongshu
description: "小红书创作者 CDP 执行工具。负责：发布（publish_pipeline.py）、竞品搜索、数据看板、评论回复、NLM 知识采集。不负责内容创作，由 wemedia 编排层调用。"
---

# xiaohongshu：全平台创作者运营 Skill

> 当前激活执行：小红书
> 联动接入：抖音（由独立 douyin skill 承担发布执行，但已纳入 wemedia 编排）
> 规划接入：X、TikTok

融合两套能力：
- **自媒体执行（工具层）**：Python CDP 脚本（发布、搜索、数据看板、评论、多账号）
- **运营 SOP** + 风控 + 爆款复刻流程

## 职责边界

**xiaohongshu = 自媒体执行（工具层），wemedia = 自媒体运营（端到端负责）**

| 归 xiaohongshu（自媒体执行） | 归 wemedia（自媒体运营） |
|---------------|-----------|
| 小红书 CDP 脚本执行（publish_pipeline / cdp_publish）| 选题发现与评估 |
| 竞品数据采集（search-feeds）| 内容队列维护（HOT/EVERGREEN/SERIES）|
| 数据看板（content-data）| Publishability Gate |
| 评论回复 | Constitution-First 内容创作 |
| 登录管理 | 正文/标题/标签生成 |
| NLM 知识采集管道（media-research notebook）| 知识查询（wemedia Step 2） |
| 平台规则驱动 | 平台内容适配（格式/结构）|

**main 的角色**：编排中心 + 监控，不直接生成内容。

**发布主体约束（更新后）**：
- 小红书：**由 main 在 Step 7.5 调用** `publish_pipeline.py`
- 抖音：由 main 在 Step 7.5 调用独立 `douyin` skill
- wemedia subagent 不直接执行任何平台发布脚本

## Required Read

按需读取：
- `references/xhs-publish-flows.md` — 发布流程分支
- `references/xhs-comment-ops.md` — 评论风控 SOP
- `references/xhs-runtime-rules.md` — 运行规则
- `references/xhs-viral-copy-flow.md` — 爆款复刻
- `references/xhs-eval-patterns.md` — JS 提取模板（browser fallback 时用）
- `examples/reply-examples.md` — 评论回复范例

## 环境

- **Python 脚本目录**：`~/.openclaw/skills/xiaohongshu/scripts/`
- **CDP 端口**：`9223`（与 OpenClaw 浏览器 18800 不冲突）
- **Chrome Profile**：`~/Library/Application Support/XiaohongshuProfiles/`
- **临时文件**：`/tmp/xhs_publish/`
- **依赖**：`requests` + `websockets`（已安装）

## 文件路径约定（与 wemedia 的接口）

| 用途 | 路径 | 写入者 | 读者 |
|------|------|--------|------|
| 内容草稿 | `~/.openclaw/workspace/intel/collaboration/media/wemedia/drafts/{A\|B\|C}/{标识}.txt` | wemedia agent | main / xiaohongshu |
| 配图 | `~/.openclaw/workspace/intel/collaboration/media/images/{A\|B\|C}/{标识}_sq.jpg` | wemedia subagent | main / xiaohongshu |
| 发布命令 | `cd ~/.openclaw/skills/xiaohongshu && python3 scripts/publish_pipeline.py ...` | main | xiaohongshu |
| 竞品数据 | `/tmp/xhs_publish/search_{keyword}_{timestamp}.json` | xiaohongshu | main 分析 |

> wemedia 负责生成草稿和配图；main 负责在 Step 7.5 调用执行工具。

## Hard Rules

1. **⛔ 未经晨星确认，绝不调用发布命令**
2. 登录操作必须有窗口（不能 headless），扫码由晨星完成
3. 评论回复遵循 `xhs-comment-ops.md` 风控（单次 1 条，间隔 8-15s）
4. source 删除前必须二次确认 ID 类型（notebook ID ≠ source ID）
5. 竞品扫描限频：每周最多 2 次，避免风控
6. 脚本失败不阻断流水线：Warning → 降级 → 继续
7. **发布主体**：main 调用执行脚本；wemedia subagent 不直接发平台
8. 抖音发布不在本 skill 内执行，改由独立 `douyin` skill 负责

## 路由规则

根据用户意图判断调用什么：

### 小红书发布
```bash
cd ~/.openclaw/skills/xiaohongshu
python3 scripts/publish_pipeline.py \
  --title "标题" --content "正文" \
  --image-urls "https://..." "https://..." \
  --headless
```
- 加 `--preview` 只填表不发布（预览模式）
- 不加 `--preview` 默认填表后自动发布
- 视频发布加 `--video /path/to/video.mp4`

### 搜索竞品
```bash
cd ~/.openclaw/skills/xiaohongshu
python3 scripts/cdp_publish.py --headless search-feeds --keyword "关键词"
```
> 注意：`--headless` 是全局参数，必须放在子命令前面。
- 支持 `--sort-by 最热|最新|综合`
- 返回结构化 JSON

### 笔记详情 + 评论
```bash
cd ~/.openclaw/skills/xiaohongshu
python3 scripts/cdp_publish.py --headless get-feed-detail \
  --feed-id <id> --xsec-token <token> \
  --load-all-comments --limit 20
```

### 数据看板
```bash
cd ~/.openclaw/skills/xiaohongshu
python3 scripts/cdp_publish.py --headless content-data --csv-file /tmp/xhs_data.csv
```

### 评论检查
```bash
cd ~/.openclaw/skills/xiaohongshu
python3 scripts/cdp_publish.py --headless get-notification-mentions
```

### 评论回复
```bash
cd ~/.openclaw/skills/xiaohongshu
python3 scripts/cdp_publish.py --headless respond-comment \
  --feed-id <id> --xsec-token <token> \
  --content "回复内容" --comment-author "用户名"
```
- ⚠️ 必须遵循 `references/xhs-comment-ops.md` 的风控规则
- 单次只回复 1 条，间隔 8-15 秒

### 登录 / 账号管理
```bash
# 首次登录（需要有窗口 Chrome，分两步）
# 步骤 1: 启动独立 Chrome 实例
"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  --remote-debugging-port=9223 \
  --user-data-dir="$HOME/Library/Application Support/XiaohongshuProfiles/default" \
  --no-first-run --no-default-browser-check \
  "https://creator.xiaohongshu.com" &
# 步骤 2: 晨星扫码登录创作者中心 + 主站（两个域需要分别登录）
```

## NLM 知识引擎集成（方案 C+）

### 知识采集管道

| 来源 | 采集方式 | 频率 | 喂入 NLM |
|------|---------|------|---------|
| X 帖子 | browser 抓取搜索/KOL | 每日 | ✅ |
| 小红书竞品 | `search-feeds` 脚本 | 每周 2 次 | ✅ |
| 技术博客 | `web_fetch` | 热点触发 | ✅ |
| 自家数据 | `content-data` 脚本 | 每日 | ✅ |

> **media-research notebook 是共享资源**：由 xiaohongshu 维护，wemedia 在 Step 2 创作前置链中调用查询。xiaohongshu 负责采集和写入，wemedia 负责查询和消费。

## 与自媒体流水线 v1.1 的集成

| Step | 改造 |
|------|------|
| Step 0 | gemini 扫描时可调 `search-feeds` 获取竞品 |
| Step 2 | 新增 NLM 深研环节（M/L 级） |
| Step 3 | wemedia 创作输入包含 NLM 知识简报 |
| Step 6 | wemedia 输出平台适配产物；小红书命令模板可由 main 拼装 |
| Step 7 | 晨星确认后再进入发布 |
| Step 7.5 | main 调 `publish_pipeline.py` 发布小红书；抖音改调独立 douyin skill |
| Step 8 | `content-data` 抓数据，写入日结，喂 NLM |

---

## 与 wemedia / douyin Skill 的联动关系

**本质**：xiaohongshu = 自媒体执行（工具层），wemedia = 自媒体运营（策略层），douyin = 抖音专用执行层。

**xiaohongshu 提供的共享资源**：
- `media-research` notebook（wemedia Step 2 查询用；**禁止用于生图**）
- `publish_pipeline.py`（由 main 在 Step 7.5 调用，负责小红书发布）
- 竞品数据文件（main 分析后传递结论给 wemedia）
- NotebookLM 配图知识与辅助流程

**抖音相关边界**：
- 抖音已纳入 wemedia 编排，但发布执行不由本 skill 处理
- 双封面、音乐、审核等待由独立 `douyin` skill 承担
- xiaohongshu 无需新增抖音执行脚本，只需保持边界清晰，避免与 douyin skill 重叠

**main 的监控职责**：
```text
wemedia 交付 → main 执行 Step 7.5 → xiaohongshu / douyin 完成平台发布 → main 监控群通知兜底
```