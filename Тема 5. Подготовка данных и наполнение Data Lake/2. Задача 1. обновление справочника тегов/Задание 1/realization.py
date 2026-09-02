from datetime import date, timedelta


def input_paths(dt: str, depth: int):
    dt = date.fromisoformat(dt)
    res = []
    for _ in range(depth):

        res.append(f'/user/s19290263/data/events/date={dt}/event_type=message')
        dt = dt - timedelta(1)
    return res


print(input_paths('2026-08-30', 5))
