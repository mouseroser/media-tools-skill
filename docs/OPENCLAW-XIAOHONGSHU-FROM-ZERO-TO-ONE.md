# 小红书全链路 OpenClaw 从 0 到 1

> 目标：用 OpenClaw 实现小红书内容运营的半自动化——从选题到发布，全程 agent 驱动。
> 预计投入：首次配置 2-3 小时，日常运营每天 4-7 分钟确认。

---

## 系统架构

```
感知层（采集）
  ├─ 小红书 CDP 脚本（竞品数据、搜索、发布）
  ├─ OpenClaw Browser（X 热门帖子采集）
  └─ Cron 定时任务（自动触发）

知识层（分析）
  ├─ NLM media-research notebook（竞品分析、内容洞察）
  └─ NLM query（内容方向建议）

执行层（发布）
  ├─ CDP 自动化发布脚本
  └─ 人工确认门控（晨星最终拍板）

编排层（调度）
  └─ OpenClaw main agent（总指挥）
```

---

## 准备条件

### 硬件
- macOS（Windows/Linux 可参考，需调整路径）
- Chrome 已安装

### 软件依赖
```bash
# 1. OpenClaw（已安装跳过）
#    参考：https://docs.openclaw.ai/getting-started

# 2. Python 3（已有跳过）
python3 --version  # 需 3.10+

# 3. Chrome ChromeDriver（CDP 用）
#    CDP 不需要独立 ChromeDriver，直接用系统 Chrome 即可

# 4. 小红书账号
#    - 手机号注册
#    - 创作者认证（非必须，但有流量倾斜）
```

---

## 安装步骤

### Step 1：安装 xiaohongshu skill

```bash
# 克隆两个项目并融合
SKILLS_DIR=~/.openclaw/skills/xiaohongshu
mkdir -p $SKILLS_DIR

# 从 XiaohongshuSkills 获取执行层
git clone https://github.com/white0dew/XiaohongshuSkills.git /tmp/xhs-scripts
cp -r /tmp/xhs-scripts/scripts/ $SKILLS_DIR/
cp /tmp/xhs-scripts/references/*.md $SKILLS_DIR/references/
cp /tmp/xhs-scripts/examples/*.md $SKILLS_DIR/examples/
cp /tmp/xhs-scripts/config/*.example $SKILLS_DIR/config/

# 从 xiaohongshu-ops 获取策略层
git clone https://github.com/Xiangyu-CAS/xiaohongshu-ops-skill.git /tmp/xhs-ops
cp /tmp/xhs-ops/references/*.md $SKILLS_DIR/references/
cp /tmp/xhs-ops/examples/*.md $SKILLS_DIR/examples/

# 安装 Python 依赖
cd $SKILLS_DIR
pip3 install -r requirements.txt

# 验证
python3 scripts/cdp_publish.py --help
```

### Step 2：macOS 适配（关键！）

**问题：Chrome 在 macOS 上绑定 IPv6，导致脚本默认连接失败。**

```bash
# 修改 CDP 端口（避开主 Chrome 占用的 9222）
# 编辑 $SKILLS_DIR/scripts/cdp_publish.py 第 98 行
# CDP_PORT = 9222  →  CDP_PORT = 9223

# 编辑 $SKILLS_DIR/scripts/chrome_launcher.py 第 20 行
# CDP_PORT = 9222  →  CDP_PORT = 9223

# 修改 is_port_open() 函数，支持 IPv6 检测
# 将 socket.AF_INET 改为 socket.AF_INET6
```

**验证适配：**
```bash
# 确认主 Chrome 端口
lsof -i :9222 | grep LISTEN
# 如果有输出，说明 9222 被占用，改为 9223

# 测试端口
curl http://localhost:9223/json/version
# 如果返回 Chrome 版本信息，说明端口可用
```

### Step 3：Chrome 实例配置

小红书需要独立的浏览器 profile（和日常 Chrome 隔离）：

```bash
# 创建独立 profile 目录
mkdir -p ~/Library/Application\ Support/XiaohongshuProfiles/default

# 手动启动一次小红书 Chrome（首次需要扫码登录）
"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  --remote-debugging-port=9223 \
  --user-data-dir="$HOME/Library/Application Support/XiaohongshuProfiles/default" \
  --no-first-run --no-default-browser-check \
  "https://creator.xiaohongshu.com"
```

**登录两个域（都需要！）：**
1. `creator.xiaohongshu.com`（创作者中心）
2. `www.xiaohongshu.com`（主站，搜索/互动用）

### Step 4：NLM 知识引擎配置

```bash
# 安装 NotebookLM CLI
# 参考：https://github.com/notebooklm/notebooklm-cli

# 认证
notebooklm auth check

# 创建 media-research notebook
notebooklm create "Media Research"
# 保存返回的 notebook ID

# 添加到 notebooks.json
cat >> ~/.openclaw/skills/notebooklm/config/notebooks.json << 'EOF'
,
  "media-research": {
    "id": "<你的notebook ID>",
    "description": "Cross-platform media research for content strategy"
  }
EOF
```

### Step 5：配置 Cron 定时任务

```bash
# 竞品扫描（周二/五 10:00）
openclaw cron add \
  --name "xhs-competitor-scan" \
  --schedule "0 10 * * 2,5" \
  --tz "Asia/Shanghai" \
  --session-target isolated \
  --payload-kind agentTurn \
  --payload-message "执行小红书竞品扫描..."

# X KOL 扫描（周一/四 07:30）
openclaw cron add \
  --name "x-kol-scan" \
  --schedule "30 7 * * 1,4" \
  ...

# 发布数据同步（每天 21:00）
openclaw cron add \
  --name "xhs-content-data-sync" \
  --schedule "0 21 * * *" \
  ...

# NLM source 清理（周日 03:00）
openclaw cron add \
  --name "nlm-media-source-cleanup" \
  --schedule "0 3 * * 0" \
  ...
```

---

## 使用流程

### 日常运营（每天 4-7 分钟）

```
1. 早上 → Cron 自动扫描竞品 + X 热门
2. 上午 → 我（main agent）分析数据，提出今日选题建议
3. 你 → 确认选题（回复"做"）
4. 我 → 调 wemedia 流水线创作内容
5. 我 → 推送预览到你，你确认
6. 你 → 回复"发"授权发布
7. 我 → 执行发布脚本
8. 晚上 → Cron 自动同步发布数据到 NLM
```

### 发布命令参考

```bash
cd ~/.openclaw/skills/xiaohongshu

# 检查登录状态
python3 scripts/cdp_publish.py check-login

# 发布内容（preview 模式，不真发）
python3 scripts/publish_pipeline.py \
  --title "你的标题" \
  --content "你的正文" \
  --image-urls "https://图片URL1,https://图片URL2" \
  --preview

# 正式发布（去掉 --preview）
python3 scripts/publish_pipeline.py \
  --title "你的标题" \
  --content "你的正文" \
  --image-urls "https://图片URL1"
```

---

## 常见问题排查

### Q1: `CDPError: Cannot reach Chrome`
**原因**：Chrome 未启动，或端口被占用。
**解决**：
```bash
# 启动独立 Chrome
python3 scripts/chrome_launcher.py

# 验证端口
curl http://localhost:9223/json/version
```

### Q2: `bind() failed: Address already in use`
**原因**：端口 9222/9223 已被其他 Chrome 占用。
**解决**：换一个空闲端口（如 9223），修改 `CDP_PORT` 值。

### Q3: 搜索返回 0 条结果
**原因**：小红书主站未登录（和创作者中心是独立的）。
**解决**：用 Chrome 手动访问 `www.xiaohongshu.com` 扫码登录。

### Q4: NLM source 添加失败
**原因**：notebook ID 无效（notebook 被删除）。
**解决**：
```bash
# 检查 notebook 列表
notebooklm list

# 新建 notebook
notebooklm create "Media Research"

# 更新 notebooks.json
```

### Q5: Cron 任务执行超时
**原因**：gemini-3.1-pro-preview 处理慢。
**解决**：给 cron 任务加长 timeout，或切换更快模型。

---

## 维护

### Chrome 实例保持
每天运营不需要重启 Chrome，它会保持登录状态。
如果第二天发现需要重新登录，运行：
```bash
python3 scripts/chrome_launcher.py
```

### NLM source 清理
NLM 有 source 数量上限（约 50/notebook）。每周日 03:00 自动清理。

手动清理：
```bash
# 查看 source 列表
notebooklm source list -n <notebook_id>

# 删除最旧的 source
notebooklm source remove -n <notebook_id> <source_id>
```

---

## 文件结构

```
~/.openclaw/skills/xiaohongshu/
├── SKILL.md              ← 路由规则 + 使用说明
├── persona.md            ← 账号人设定位
├── scripts/
│   ├── cdp_publish.py    ← 核心 CDP 脚本（登录/搜索/发布/评论）
│   ├── chrome_launcher.py ← Chrome 启动/检测
│   ├── publish_pipeline.py ← 发布流程编排
│   ├── image_downloader.py ← 图片下载
│   ├── account_manager.py ← 多账号管理
│   └── feed_explorer.py  ← 竞品内容探索
├── references/           ← 运营 SOP + 风控规则
├── examples/            ← 评论回复范例
└── config/               ← 账号配置模板
```
