import asyncio
import json
import subprocess
import uuid
import websockets


async def send_to_minecraft(websocket, text):
    text = text.replace("[", "【").replace("]", "】")
    text = text.strip()
    if not text:
        return
    chunks = [text[i : i + 350] for i in range(0, len(text), 350)]
    for chunk in chunks:
        payload = {
            "header": {
                "version": 1,
                "requestId": str(uuid.uuid4()),
                "messageType": "commandRequest",
                "messagePurpose": "commandRequest",
            },
            "body": {
                "version": 1,
                "origin": {"type": "player"},
                "commandLine": f"/say {chunk}",
            },
        }
        try:
            await websocket.send(json.dumps(payload))
        except Exception as e:
            print(f"{e}")
        await asyncio.sleep(0.05)


async def handle_minecraft(websocket):
    print("\n正在不紧不慢不快不慢的建立链接")
    loop = asyncio.get_running_loop()
    current_process = None

    async def read_output_task(p, ws):
        nonlocal current_process
        try:
            while True:
                line_bytes = await loop.run_in_executor(None, p.stdout.readline)
                if not line_bytes:
                    break

                line = line_bytes.decode("utf-8", errors="ignore").strip()
                if line:
                    print(f"{line}")
                    await send_to_minecraft(ws, line)
        except Exception as e:
            print(f"{e}")
        finally:
            p.wait()
            if current_process == p:
                current_process = None
                print("完成！久久！完成！")
                await send_to_minecraft(ws, "完成！久久！完成！")

    try:
        subscribe_payload = {
            "header": {
                "version": 1,
                "requestId": str(uuid.uuid4()),
                "messageType": "commandRequest",
                "messagePurpose": "subscribe",
            },
            "body": {"eventName": "PlayerMessage"},
        }
        await websocket.send(json.dumps(subscribe_payload))

        async for message in websocket:
            try:
                data = json.loads(message)
                header = data.get("header", {})
                body = data.get("body", {})
                purpose = header.get("messagePurpose")
                if purpose == "commandResponse":
                    status_code = body.get("statusCode")
                    status_msg = body.get("statusMessage")
                    print(f"{status_code}   {status_msg}")
                    continue

                if (
                    purpose == "event"
                    and header.get("eventName") == "PlayerMessage"
                ):
                    msg_text = body.get("message", "").strip()

                    if msg_text.startswith("!termux "):
                        content = msg_text[8:].strip()
                        if content == "^c":
                            if current_process is not None:
                                try:
                                    current_process.kill()
                                    await send_to_minecraft(
                                        websocket, "完成！久久！完成！"
                                    )
                                except Exception as e:
                                    print(f"{e}")
                            else:
                                await send_to_minecraft(
                                    websocket, "^c"
                                )
                            continue

                        if current_process is None:
                            print(f"{content}")
                            try:
                                current_process = subprocess.Popen(
                                    f"{content} 2>&1",
                                    shell=True,
                                    stdout=subprocess.PIPE,
                                    stdin=subprocess.PIPE,
                                    stderr=subprocess.STDOUT,
                                )
                                asyncio.create_task(
                                    read_output_task(current_process, websocket)
                                )
                            except Exception as e:
                                print(f"{e}")
                                await send_to_minecraft(
                                    websocket, f"{e}"
                                )
                                current_process = None
                        else:
                            print(f"{content}")
                            try:
                                current_process.stdin.write(
                                    (content + "\n").encode("utf-8")
                                )
                                current_process.stdin.flush()
                            except Exception as e:
                                print(f"{e}")

            except json.JSONDecodeError:
                continue
            except Exception as e:
                print(f"{e}")

    except websockets.exceptions.ConnectionClosed as e:
        print(f"中断")


async def main():
    async with websockets.serve(handle_minecraft, "0.0.0.0", 8000):
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
