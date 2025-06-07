# TikTokLiveInfo 提取工具

本项目包含一个脚本 `dump_tiktoklive_info.py`，用于测试并输出 TikTokLiveClient 中的关键数据结构：

- **Room Info**：直播房间的详细信息
- **Gift Info**：当前直播可使用的礼物列表

## 准备工作

1. 安装依赖：
   ```bash
   pip install -r requirements.txt
   ```

2. 如果需要通过登录获取完整的房间信息（如 18+ 限制），请设置环境变量：
   ```bash
   export SIGN_API_KEY=你的Euler签名服务API_KEY
   export SIGN_API_URL=https://你的签名服务地址
   ```

## 使用方式

在命令行中运行：

```bash
python dump_tiktoklive_info.py <unique_id>
```

例如：

```bash
python dump_tiktoklive_info.py diynana_yuri
```

脚本会依次输出：

1. **Room Info** （JSON 格式）
2. **Gift Info** （JSON 格式）

## 输出示例

下面是示例输出（请根据实际运行结果进行替换）：

### === Room Info ===
```json
{
  "room_id": "7512984430680525576",
  "status": 2,
  "stream_url": "https://example.com/live/stream.mp4",
  "title": "直播间标题",
  "cover_url": "https://example.com/cover.jpg",
  "author_unique_id": "diynana_yuri",
  "author_nickname": "diy_nana",
  "user_count": 1234,
  "share_url": "https://www.tiktok.com/@diynana_yuri/live",
  ...
}
```

### === Gift Info ===
```json
{
  "gifts": [
    {
      "id": 1,
      "name": "喜欢",
      "coin": 10,
      "diamond": 0,
      "gift_type": 1,
      "repeatable": false,
      ...
    },
    {
      "id": 2,
      "name": "鲜花",
      "coin": 50,
      "diamond": 0,
      "gift_type": 1,
      "repeatable": false,
      ...
    }
  ],
  "can_express": true,
  "show_rewarded_gifts": false
}
```

请运行脚本获取你所在直播的真实数据，并将上面示例替换为实际输出。 