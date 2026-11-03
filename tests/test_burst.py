"""Seconds mode: one run polls every poll_interval_sec for a burst window."""
from __future__ import annotations

from typing import Any

from worker import main, throttle

from .test_main import FakeProvider, _train, _watch_dict


def _seconds_config(sec: int | None = 10, **settings: Any) -> dict:
    s: dict[str, Any] = {"poll_interval_mode": "seconds", **settings}
    if sec is not None:
        s["poll_interval_sec"] = sec
    return {"watches": [_watch_dict()], "settings": s}


# ── throttle ────────────────────────────────────────────────────────────────

def test_burst_interval_floors_at_ten_seconds():
    assert throttle.burst_interval_sec(_seconds_config(3)) == 10
    assert throttle.burst_interval_sec(_seconds_config(None)) == 10
    assert throttle.burst_interval_sec(_seconds_config(25)) == 25


def test_burst_interval_is_zero_outside_seconds_mode():
    assert throttle.burst_interval_sec({"settings": {"poll_interval_min": 10}}) == 0
    assert throttle.burst_interval_sec({}) == 0


def test_seconds_mode_never_throttles_a_tick():
    cfg = _seconds_config()
    state = {"last_run": "2026-05-30T10:00:00Z", "next_poll_at": "2026-05-30T11:00:00Z"}
    assert throttle.will_poll(cfg, state, "repository_dispatch", "2026-05-30T10:00:05Z")


def test_seconds_mode_is_not_overridden_by_urgent_cadence():
    cfg = _seconds_config()
    cfg["watches"][0]["date"] = "2026-05-30"
    mode, _, _, urgent = throttle.effective_cadence(cfg, "2026-05-30T01:00:00Z")
    assert (mode, urgent) == ("seconds", False)


# ── run_watches ─────────────────────────────────────────────────────────────

def _run(provider: FakeProvider, cfg: dict, state: dict, notes: list, **kw: Any) -> int:
    return main.run_watches(
        cfg, state,
        providers={"korail": provider},
        creds={"korail": ("id", "pw")},
        notify_fn=lambda w, trains, **_: notes.append([t.raw_id for t in trains]),
        now_iso="2026-05-14T00:00:00Z",
        event_name="repository_dispatch",
        **kw,
    )


class CountingProvider(FakeProvider):
    def __init__(self, *a: Any, **kw: Any) -> None:
        super().__init__(*a, **kw)
        self.logins = 0

    def login(self, user_id: str, password: str) -> None:
        self.logins += 1
        super().login(user_id, password)


def test_session_is_reused_across_rounds():
    p = CountingProvider("korail", search_results=[])
    logged_in: set[str] = set()
    state: dict = {"watches": {}}
    for _ in range(3):
        _run(p, _seconds_config(), state, [], logged_in=logged_in)
    assert p.logins == 1
    assert len(p.searches) == 3


def test_all_searches_failing_drops_the_session():
    p = CountingProvider("korail", raise_on_search=RuntimeError("boom"))
    logged_in: set[str] = set()
    _run(p, _seconds_config(), {"watches": {}}, [], logged_in=logged_in)
    assert logged_in == set()


def test_burst_round_does_not_renotify():
    cfg = _seconds_config(renotify_while_available=True)
    p = FakeProvider("korail", search_results=[_train("r1")])
    state: dict = {"watches": {}}
    notes: list = []
    assert _run(p, cfg, state, notes) == 1
    assert _run(p, cfg, state, notes, burst_round=True) == 0
    assert notes == [["r1"]]


def test_returns_new_train_count():
    p = FakeProvider("korail", search_results=[_train("a"), _train("b")])
    assert _run(p, _seconds_config(), {"watches": {}}, []) == 2


# ── _burst loop ─────────────────────────────────────────────────────────────

class FakeClock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t

    def sleep(self, d: float) -> None:
        self.t += d


def _burst(cfg: dict, poll, monkeypatch, *, configs: list | None = None, window: float = 170):
    seq = iter(configs) if configs is not None else None
    monkeypatch.setattr(main, "load_config", lambda: next(seq) if seq else cfg)
    saves: list = []
    monkeypatch.setattr(main, "_save_state", lambda c, s, n: saves.append(n))
    clock = FakeClock()
    main._burst(cfg, {}, poll, "t0", window_sec=window, clock=clock, sleep=clock.sleep)
    return clock, saves


def test_burst_polls_every_interval_within_the_window(monkeypatch):
    monkeypatch.setattr(main, "load_config", _seconds_config)
    monkeypatch.setattr(main, "_save_state", lambda *a: None)
    clock = FakeClock()
    times: list[float] = []

    def poll(c, first):
        times.append(clock())
        return 0, "t", True

    main._burst(_seconds_config(), {}, poll, "t0", clock=clock, sleep=clock.sleep)
    assert times == [10.0 * k for k in range(1, 18)]   # 10s … 170s


def test_burst_stops_when_mode_switched_off(monkeypatch):
    calls: list = []
    off = {"watches": [_watch_dict()], "settings": {"poll_interval_mode": "fixed"}}
    _burst(_seconds_config(), lambda c, f: calls.append(f) or (0, "t", True),
           monkeypatch, configs=[_seconds_config(), off])
    assert calls == [False]


def test_burst_stops_without_a_session(monkeypatch):
    calls: list = []
    _burst(_seconds_config(), lambda c, f: calls.append(f) or (0, "t", False), monkeypatch)
    assert len(calls) == 1


def test_burst_saves_state_only_when_seats_found(monkeypatch):
    found = iter([0, 2, 0])
    _, saves = _burst(_seconds_config(), lambda c, f: (next(found), "t", True),
                      monkeypatch, window=30)
    assert len(saves) == 1


def test_burst_keeps_last_config_when_reload_fails(monkeypatch):
    def boom():
        raise main.remote.RemoteError("down")
    monkeypatch.setattr(main, "load_config", boom)
    monkeypatch.setattr(main, "_save_state", lambda *a: None)
    calls: list = []
    clock = FakeClock()
    main._burst(_seconds_config(), {}, lambda c, f: calls.append(c) or (0, "t", True),
                "t0", window_sec=30, clock=clock, sleep=clock.sleep)
    assert len(calls) == 3
