# xiaohongshu

自媒体执行工具集 — OpenClaw Agent Skill

> 当前激活：**小红书**  
> 规划接入：X、抖音、TikTok

## 定位

**自媒体执行（工具层）**：提供 CDP 自动化脚本 + 运营 SOP + NLM 知识引擎集成。

与 [wemedia](https://github.com/mouseroser/wemedia-skill)（自媒体运营）配合使用：

```
wemedia（自媒体运营：决定发什么、什么时候发）
    ↓
main（编排调度）
    ↓
xiaohongshu（自媒体执行：发布、数据采集、知识管道）
```

## 能力

| 功能 | 脚本 | 说明 |
|------|------|------|
| 📝 发布 | `publish_pipeline.py` | 一键发布（标题+正文+配图），支持预览模式 |
| 🔍 搜索竞品 | `cdp_publish.py search-feeds` | 关键词搜索，支持排序 |
| 📊 数据看板 | `cdp_publish.py content-data` | 导出笔记数据 CSV |
| 💬 评论回复 | `cdp_publish.py respond-comment` | 风控安全的评论互动 |
| 🔐 登录管理 | `cdp_publish.py check-login` | 检查/刷新登录态 |
| 📰 首页推荐 | `cdp_publish.py list-feeds` | 获取首页推荐列表 |

## 知识引擎（NLM 集成）

通过 NotebookLM `media-research` notebook 提供知识支持：

- **采集**：X 帖子、小红书竞品、技术博客、自家数据
- **查询**：选题差异化分析、内容策略建议
- **共享**：xiaohongshu 维护 notebook，wemedia Step 2 前置链调用查询

## 快速开始

### 环境要求

- Python 3.10+
- Chrome（CDP 远程调试，端口 9223）
- `pip install -r requirements.txt`

### 发布示例

```bash
cd ~/.openclaw/skills/xiaohongshu
python3 scripts/publish_pipeline.py \
  --title "标题" \
  --content "正文内容" \
  --image-urls "/path/to/image.jpg" \
  --headless
```

加 `--preview` 只填表不发布。

### 搜索竞品

```bash
python3 scripts/cdp_publish.py --headless search-feeds --keyword "AI编程"
```

## 目录结构

```
xiaohongshu/
├── SKILL.md              # Agent skill 定义（职责边界、路由规则、Hard Rules）
├── README.md             # 本文件
├── persona.md            # 人设配置（待填写）
├── requirements.txt      # Python 依赖
├── scripts/              # CDP 自动化脚本
│   ├── publish_pipeline.py    # 发布流水线
│   ├── cdp_publish.py         # CDP 核心（搜索/数据/评论/登录）
│   ├── chrome_launcher.py     # Chrome 实例管理
│   ├── feed_explorer.py       # 笔记探索
│   ├── image_downloader.py    # 图片下载
│   ├── account_manager.py     # 多账号管理
│   └── run_lock.py            # 运行锁
├── references/           # 运营 SOP 文档
│   ├── xhs-publish-flows.md      # 发布流程分支
│   ├── xhs-comment-ops.md        # 评论风控 SOP
│   ├── xhs-runtime-rules.md      # 运行规则
│   ├── xhs-viral-copy-flow.md    # 爆款复刻流程
│   └── xhs-eval-patterns.md      # JS 提取模板
├── config/               # 配置文件
│   └── accounts.json.example     # 多账号配置示例
└── examples/             # 使用示例
    └── reply-examples.md         # 评论回复范例
```

## Hard Rules

1. ⛔ **未经确认，绝不调用发布命令**
2. 登录操作必须有窗口（不能 headless），需人工扫码
3. 评论回复遵循风控规则（单次 1 条，间隔 8-15s）
4. 竞品扫描限频：每周最多 2 次
5. 脚本失败不阻断流水线：Warning → 降级 → 继续
6. **发布脚本（publish_pipeline.py）是 main agent 的专属工具**

## 与 wemedia 的关系

| xiaohongshu（自媒体执行） | wemedia](https://github.com/mouseroser/wemedia-skill)（自媒体运营） |
|----------------------|------------------|
| CDP 脚本执行 | 选题发现与评估 |
| 竞品数据采集 | 内容队列维护 |
| 数据看板 | Publishability Gate |
| 评论回复 | Constitution-First 创作 |
| NLM 知识采集管道 | 正文/标题/标签生成 |

## License

Private skill for OpenClaw agent system.
