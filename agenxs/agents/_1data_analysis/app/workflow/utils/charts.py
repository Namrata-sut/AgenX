from __future__ import annotations

import os
from typing import Dict, List, Optional

import matplotlib.pyplot as plt
import polars as pl

from .io import ensure_dir


def save_bar_chart(data: pl.DataFrame, x: str, y: str, out_path: str, title: Optional[str] = None) -> str:
    ensure_dir(os.path.dirname(out_path))
    xs = data[x].to_list()
    ys = data[y].to_list()

    plt.figure()
    plt.bar(xs, ys)
    if title:
        plt.title(title)
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()
    return out_path


def save_line_chart(data: pl.DataFrame, x: str, y: str, out_path: str, title: Optional[str] = None) -> str:
    ensure_dir(os.path.dirname(out_path))
    xs = data[x].to_list()
    ys = data[y].to_list()

    plt.figure()
    plt.plot(xs, ys)
    if title:
        plt.title(title)
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()
    return out_path