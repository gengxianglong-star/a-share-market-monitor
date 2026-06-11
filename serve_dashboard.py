"""本地启动复盘看板。用法: python serve_dashboard.py"""
from __future__ import annotations

import http.server
import socketserver
import webbrowser
from pathlib import Path

PORT = 8765
ROOT = Path(__file__).resolve().parent


class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(ROOT), **kwargs)


class ReuseTCPServer(socketserver.TCPServer):
    allow_reuse_address = True


def main() -> None:
    url = f"http://127.0.0.1:{PORT}/index.html"
    print("=" * 56)
    print(f"看板地址: {url}")
    print("不要双击 index.html，必须通过上面的地址打开。")
    print("按 Ctrl+C 停止服务")
    print("=" * 56)
    try:
        webbrowser.open(url)
    except Exception:
        pass
    try:
        with ReuseTCPServer(("127.0.0.1", PORT), Handler) as httpd:
            httpd.serve_forever()
    except OSError as exc:
        print(f"启动失败（端口 {PORT} 可能被占用）: {exc}")
        print(f"请关闭占用进程后重试，或直接访问 {url}")
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
