"""common/env.py tests.
"""
import pytest

from common.env import env_bool, env_int, env_list, env_str


@pytest.mark.parametrize("raw", ["1", "true", "TRUE", "True", "yes", "on"])
def test_truthy_values(monkeypatch, raw):
    monkeypatch.setenv("X", raw)
    assert env_bool("X") is True


@pytest.mark.parametrize("raw", ["0", "false", "no", "off", "nonsense"])
def test_falsy_values(monkeypatch, raw):
    monkeypatch.setenv("X", raw)
    assert env_bool("X", default=True) is False


def test_empty_falls_back_to_default_rather_than_false(monkeypatch):
    monkeypatch.setenv("X", "")
    assert env_bool("X", default=True) is True
    assert env_bool("X", default=False) is False


def test_whitespace_only_is_also_empty(monkeypatch):
    monkeypatch.setenv("X", "   ")
    assert env_bool("X", default=True) is True


def test_unset_uses_default(monkeypatch):
    monkeypatch.delenv("X", raising=False)
    assert env_bool("X", default=True) is True
    assert env_bool("X") is False


def test_surrounding_whitespace_is_tolerated(monkeypatch):
    monkeypatch.setenv("X", "  true \n")
    assert env_bool("X") is True


def test_list_splits_and_strips(monkeypatch):
    monkeypatch.setenv("X", " a.example.com , b.example.com ")
    assert env_list("X") == ["a.example.com", "b.example.com"]


def test_list_drops_blank_entries(monkeypatch):
    monkeypatch.setenv("X", "a,,b,")
    assert env_list("X") == ["a", "b"]


def test_empty_list_falls_back_to_default(monkeypatch):
    monkeypatch.setenv("X", "")
    assert env_list("X", ["fallback"]) == ["fallback"]
    assert env_list("X") == []


def test_list_default_is_copied_not_shared(monkeypatch):
    """A returned default that aliases the caller's list would let one settings
    module mutate another's."""
    monkeypatch.delenv("X", raising=False)
    default = ["a"]
    returned = env_list("X", default)
    returned.append("b")

    assert default == ["a"]


def test_str_empty_falls_back(monkeypatch):
    monkeypatch.setenv("X", "")
    assert env_str("X", "fallback") == "fallback"


def test_int_empty_falls_back(monkeypatch):
    monkeypatch.setenv("X", "")
    assert env_int("X", 587) == 587


def test_int_parses(monkeypatch):
    monkeypatch.setenv("X", " 25 ")
    assert env_int("X", 587) == 25


def test_int_rejects_garbage_rather_than_silently_defaulting(monkeypatch):
    """Avoid typos in ints (such as ports)"""
    monkeypatch.setenv("X", "not-a-number")
    with pytest.raises(ValueError):
        env_int("X", 587)
