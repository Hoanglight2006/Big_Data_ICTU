#!/usr/bin/env python3
# =============================================================================
# mapreduce/reducer.py — Hadoop Streaming Reducer
#
# NGUYÊN LÝ HOẠT ĐỘNG:
#   - Nhận input từ stdin đã được Hadoop Sort/Shuffle theo Key.
#   - Tất cả các dòng có cùng Key sẽ đứng liền kề nhau liên tục.
#   - Reducer gom nhóm (group by) theo current_key:
#       + Với hour_resp (tính trung bình): cộng tổng response_time và đếm count
#       + Với các metric khác (tính tổng): cộng dồn sum các count
#   - Khi đổi Key -> emit kết quả aggregate của Key trước đó ra stdout.
# =============================================================================

import sys

def emit_result(key, val, count=1):
    if key.startswith("hour_resp:"):
        # Tính giá trị trung bình response time: total / count
        avg_val = val / count if count > 0 else 0.0
        print(f"{key}\t{avg_val:.2f}")
    else:
        # Tổng số lượng đếm được
        print(f"{key}\t{int(val)}")

def main():
    current_key = None
    accumulated_val = 0.0
    accumulated_count = 0

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        try:
            key, val_str = line.split("\t", 1)
            val = float(val_str)
        except ValueError:
            continue

        if current_key == key:
            accumulated_val += val
            accumulated_count += 1
        else:
            if current_key is not None:
                emit_result(current_key, accumulated_val, accumulated_count)

            current_key = key
            accumulated_val = val
            accumulated_count = 1

    # Emit key cuối cùng
    if current_key is not None:
        emit_result(current_key, accumulated_val, accumulated_count)

if __name__ == "__main__":
    main()
