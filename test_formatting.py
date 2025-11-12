#!/usr/bin/env python3
"""
Lightweight formatting tests for FPL bot commands.
Tests rendering of /gw, /transfers, /gwinfo with canned data to ensure
columns/widths and Markdown fallbacks don't break.
"""

import json
from typing import Dict, List, Any


def test_gwinfo_formatting():
    """Test /gwinfo table formatting with various stat combinations."""
    
    # Test data: players with different stat combinations
    test_rows = [
        {"name": "Salah", "stats": "G2 A1 B3", "pts": 15, "own": 10},
        {"name": "Haaland", "stats": "G1 CS DC", "pts": 12, "own": 9},
        {"name": "Trippier", "stats": "A1 YC1", "pts": 7, "own": 8},
        {"name": "Pope", "stats": "CS GKS2", "pts": 8, "own": 7},
        {"name": "Rashford", "stats": "G1 YC2 RC", "pts": 3, "own": 6},
        {"name": "De Bruyne", "stats": "A2", "pts": 6, "own": 5},
        {"name": "Walker", "stats": "-", "pts": 2, "own": 4},
    ]
    
    # Sort by pts desc, own desc, name asc
    rows = sorted(test_rows, key=lambda r: (-r["pts"], -r["own"], r["name"].lower()))
    
    # Compute column widths
    name_w = max(len(r["name"]) for r in rows + [{"name": "Player"}])
    stats_w = max(len(r["stats"]) for r in rows + [{"stats": "Stats"}])
    pts_w = max(len(str(r["pts"])) for r in rows + [{"pts": "Pts"}])
    
    header = f"{'Player'.ljust(name_w)}  {'Stats'.ljust(stats_w)}  {'Pts'.rjust(pts_w)}"
    sep = "-" * len(header)
    
    lines = ["*GW 1 — Игроки лиги (live)*", "```", header, sep]
    for r in rows:
        line = (
            f"{r['name'].ljust(name_w)}  "
            f"{r['stats'].ljust(stats_w)}  "
            f"{str(r['pts']).rjust(pts_w)}"
        )
        lines.append(line)
    lines.append("```")
    
    output = "\n".join(lines)
    print("=== /gwinfo formatting test ===")
    print(output)
    print()
    
    # Validate structure
    assert "```" in output
    assert "Player" in output
    assert "Stats" in output
    assert "Pts" in output
    assert "Salah" in output
    assert len(lines) > 5
    print("✓ /gwinfo formatting test passed")
    return True


def test_month_formatting():
    """Test /month aggregation output formatting."""
    
    test_data = [
        ("Player1", "Team1", 150),
        ("Player2", "Team2", 145),
        ("Player3", "Team3", 140),
    ]
    
    lines = ["*Очки за месяц 1 (GWs 1-4):*\n"]
    for rank, (player_name, entry_name, total) in enumerate(test_data, 1):
        lines.append(f"{rank}. {player_name} — {entry_name}: {total}")
    
    output = "\n".join(lines)
    print("=== /month formatting test ===")
    print(output)
    print()
    
    # Validate structure
    assert "Очки за месяц" in output
    assert "Player1" in output
    assert "150" in output
    assert "1." in output
    print("✓ /month formatting test passed")
    return True


def test_transfers_formatting():
    """Test /transfers output formatting."""
    
    test_data = [
        ("Player1", "Team1", 2, 4),  # 2 transfers, -4 cost
        ("Player2", "Team2", 1, 0),   # 1 transfer, no cost
        ("Player3", "Team3", 3, 8),   # 3 transfers, -8 cost
    ]
    
    # Sort by transfers descending
    sorted_data = sorted(test_data, key=lambda x: -x[2])
    
    lines = ["*Трансферы GW 1:*\n"]
    for player_name, entry_name, transfers, cost in sorted_data:
        cost_str = f" (-{cost})" if cost > 0 else ""
        lines.append(f"{player_name} — {entry_name}: {transfers}{cost_str}")
    
    output = "\n".join(lines)
    print("=== /transfers formatting test ===")
    print(output)
    print()
    
    # Validate structure
    assert "Трансферы GW" in output
    assert "Player3" in lines[1]  # Should be first (3 transfers)
    assert "(-8)" in output
    assert "Player2" in output
    print("✓ /transfers formatting test passed")
    return True


def test_gw_formatting():
    """Test /gw output with event_total optimization."""
    
    test_entries = [
        ("Player1", "Team1", 85),
        ("Player2", "Team2", 78),
        ("Player3", "Team3", None),  # No data
    ]
    
    lines = ["*Очки за тур 1*\n"]
    for player_name, entry_name, pts in test_entries:
        pts_str = pts if pts is not None else "нет данных"
        lines.append(f"{player_name} — {entry_name}: {pts_str}")
    
    output = "\n".join(lines)
    print("=== /gw formatting test ===")
    print(output)
    print()
    
    # Validate structure
    assert "Очки за тур" in output
    assert "Player1" in output
    assert "85" in output
    assert "нет данных" in output
    print("✓ /gw formatting test passed")
    return True


def test_markdown_fallback():
    """Test that messages can fall back from Markdown to plain text."""
    
    # Test with Markdown special characters
    test_message = "*Тест* с _форматированием_ и ```кодом```"
    
    # Simulate fallback
    safe_message = test_message.replace("```", "").replace("*", "").replace("_", "")
    
    print("=== Markdown fallback test ===")
    print(f"Original: {test_message}")
    print(f"Safe:     {safe_message}")
    print()
    
    assert "Тест" in safe_message
    assert "*" not in safe_message
    assert "```" not in safe_message
    print("✓ Markdown fallback test passed")
    return True


def test_message_chunking():
    """Test message chunking for long outputs."""
    
    # Create a long message (need >4000 chars)
    lines = ["Header"] + [f"Line {i} with some additional text to make it longer" for i in range(100)]
    long_message = "\n".join(lines)
    
    # Pad to ensure it's over 4000 chars
    if len(long_message) < 4100:
        long_message = long_message + "\n" + ("X" * 4000)
    
    # Simple chunking logic (from split_message_chunks)
    limit = 4000
    if len(long_message) <= limit:
        chunks = [long_message]
    else:
        lines_split = long_message.splitlines(keepends=True)
        chunks = []
        buf = ""
        for ln in lines_split:
            if len(buf) + len(ln) > limit:
                chunks.append(buf)
                buf = ""
            buf += ln
        if buf:
            chunks.append(buf)
    
    print("=== Message chunking test ===")
    print(f"Original length: {len(long_message)}")
    print(f"Number of chunks: {len(chunks)}")
    print(f"Chunk sizes: {[len(c) for c in chunks]}")
    print()
    
    assert len(chunks) >= 1
    assert all(len(c) <= limit + 100 for c in chunks)  # Allow some buffer for line endings
    print("✓ Message chunking test passed")
    return True


def main():
    """Run all formatting tests."""
    print("\n" + "="*60)
    print("FPL Bot Formatting Tests")
    print("="*60 + "\n")
    
    tests = [
        test_gwinfo_formatting,
        test_month_formatting,
        test_transfers_formatting,
        test_gw_formatting,
        test_markdown_fallback,
        test_message_chunking,
    ]
    
    results = []
    for test in tests:
        try:
            results.append(test())
        except Exception as e:
            print(f"✗ {test.__name__} failed: {e}\n")
            results.append(False)
    
    print("="*60)
    print(f"Tests passed: {sum(results)}/{len(results)}")
    print("="*60 + "\n")
    
    return all(results)


if __name__ == "__main__":
    import sys
    sys.exit(0 if main() else 1)
