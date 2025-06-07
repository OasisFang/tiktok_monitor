import asyncio
import json
from TikTokLive import TikTokLiveClient
from TikTokLive.client.web.routes.fetch_room_id_live_html import FailedParseRoomIdError

async def main(unique_id: str):
    # 初始化客户端
    client = TikTokLiveClient(unique_id=unique_id)
    
    # 解析并设置房间ID（通过HTML或API获取）
    try:
        room_id = await client.web.fetch_room_id_from_html(client.unique_id)
    except FailedParseRoomIdError:
        print("HTML 解析房间ID失败，使用 API 获取")
        room_id = await client.web.fetch_room_id_from_api(client.unique_id)
    client.web.params['room_id'] = str(room_id)

    # 拉取房间信息
    room_info = await client.web.fetch_room_info()
    # 拉取礼物列表
    gift_info = await client.web.fetch_gift_list()

    # 输出结果
    print("\n=== Room Info ===")
    print(json.dumps(room_info, indent=2, ensure_ascii=False))
    print("\n=== Gift Info ===")
    print(json.dumps(gift_info, indent=2, ensure_ascii=False))

    # 写入结果到文件
    output_file = f"{unique_id}_tiktoklive_info.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump({"room_info": room_info, "gift_info": gift_info}, f, ensure_ascii=False, indent=2)
    print(f"已将结果写入文件: {output_file}")

    # 安全关闭HTTP客户端
    try:
        await client.web.close()
    except Exception:
        try:
            await client.web.httpx_client.aclose()
        except Exception:
            pass

if __name__ == '__main__':
    import sys
    if len(sys.argv) < 2:
        print("用法: python dump_tiktoklive_info.py <unique_id>")
        sys.exit(1)
    uid = sys.argv[1]
    asyncio.run(main(uid)) 