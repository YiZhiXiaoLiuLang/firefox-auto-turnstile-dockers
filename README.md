# firefox-auto-turnstile-dockers

[firefox-auto-turnstile](https://github.com/YiZhiXiaoLiuLang/firefox-auto-turnstile)
的多 worker 部署：一个网关 + 三个上游原版 worker 容器（不修改上游镜像）。

- 客户端仍调 `POST :8081/solve`，接口与单机版完全兼容
- 网关挑空闲 worker，全忙时转发给最可能先空出来的 worker（409 则换下一个）
- 响应 JSON 新增 `solver` 字段，标明本次由哪个 worker 处理
- `GET /stats` 统计页：每个 solver 的请求数、成功数、平均/最近 solve 耗时（内存统计，刷新即更新）
- 校准坐标跨 worker 共享：同一站点只需在任一 worker 人工点一次，所有 worker 自动复用
- 每个 worker 私有 `/config`：mitmproxy CA、task/result 文件互不干扰
- noVNC：worker-1→5802，worker-2→5803，worker-3→5804（每加一个 worker 顺延）
- 镜像由 GitHub Actions 构建并推送到 GHCR，本地零构建

## 部署

两个版本任选其一（同一台机器上不要同时跑，端口会冲突）：

```bash
# 3 worker 版（VNC: 5802-5804）
docker compose -f docker-compose.yml pull && docker compose -f docker-compose.yml up -d

# 8 worker 版（VNC: 5802-5809）
docker compose -f docker-compose.x8.yml pull && docker compose -f docker-compose.x8.yml up -d
```

网关镜像一次构建，两个版本共用（worker 列表由 `WORKERS` 环境变量驱动）。

首次校准：浏览器打开 `http://<host>:5802`（或 5803/5804），在该站点的验证页
上点一次复选框；网关每 3 秒把校准坐标同步给所有 worker。

## 使用

```bash
curl -X POST http://<host>:8081/solve \
  -H 'Content-Type: application/json' \
  -d '{"url":"https://target-site.example/login","sitekey":"0x4AAA...","timeout":180}'
```

响应示例：

```json
{
  "ok": true,
  "task_id": "a1b2c3d4e5f6",
  "solver": "worker-2",
  "hostname": "target-site.example",
  "token": "XXXX...",
  "elapsed": 12.3,
  "click": {"x": 640, "y": 320, "ts": 1758691200.5},
  "calibrated": true,
  "auto_clicked": true
}
```

端点：`POST /solve`、`GET /status`（聚合各 worker）、`GET /coords`（合并坐标）、
`GET /stats`（HTML 统计页）、`GET /healthz`。

## 坐标共享原理

worker 的 `/config/relay` 保持私有（task.json/result.json 不共享，避免并发
互相覆盖）。网关侧每 3 秒：拉取所有 worker 的 `GET /coords` → 按 hostname 取
最新 `ts` 合并 → 与各 worker 专属子目录中的 `coords.json` 比对，有变化才原子
写回。API 每个任务开始时才读 coords.json，且写方全部使用 tmp+rename 原子写，
同步过程不会让 worker 读到半截文件。

## 扩容

复制 compose 里的 worker 模板：改服务名/host、宿主端口顺延、在网关 `WORKERS`
里追加一项，`docker compose up -d` 即可。

## CI/CD

推送到 `main` 即触发 `.github/workflows/ci-cd.yml`：构建网关镜像
（linux/amd64 + arm64）并推送到
`ghcr.io/yizhixiaoliulang/firefox-auto-turnstile-dockers-gateway`。
