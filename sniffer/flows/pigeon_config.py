# ──────────────────────────────────────────────────────────────────────────────
# 模块用途：集中管理 Flow 的运行配置（环境变量 → dataclass）
# 说明：
#   - 把零散的 os.getenv 移到这里，便于统一查看与测试；
#   - 上层只依赖 PigeonConfig，不直接感知环境变量键名。
# ──────────────────────────────────────────────────────────────────────────────
from __future__ import annotations
import os
from dataclasses import dataclass

@dataclass(frozen=True)
class PigeonConfig:
    """Flow 的运行配置（不可变 dataclass）"""
    max_concurrency: int                 # 并发上限（与爬虫池大小一致）
    cooldown_sec: float                  # 去抖窗口（秒）
    bootstrap_pids_raw: str              # 冷启动 pid 列表（逗号分隔）
    use_current_bootstrap: bool          # 无列表时，是否回退使用 current 接口
    debug_verbose: bool                  # 调试日志开关

    @staticmethod
    def from_env() -> "PigeonConfig":
        """从环境变量读取配置并构造实例。"""
        return PigeonConfig(
            max_concurrency=int(os.getenv("PIGEON_FLOW_MAX_CONCURRENCY", "4")),
            cooldown_sec=float(os.getenv("PIGEON_FLOW_COOLDOWN_SEC", "2.0")),
            bootstrap_pids_raw=os.getenv("PIGEON_BOOTSTRAP_PIDS", ""),
            use_current_bootstrap=os.getenv("PIGEON_BOOTSTRAP_USE_CURRENT", "true").lower() == "true",
            debug_verbose=os.getenv("PIGEON_FLOW_DEBUG", "true").lower() == "true",
        )
