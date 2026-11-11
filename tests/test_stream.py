import json

import pytest

from src.stream import Scorer


def test_spikes_do_not_contaminate_other_metrics_or_their_own_baseline():
    scorer = Scorer()

    def score(metric, value):
        return scorer.score(json.dumps({"metric": metric, "value": value}))

    for value in [120, 130, 110, 125, 135, 128]:
        assert score("checkout", value) is None

    assert score("other", 980) is None
    assert score("checkout", 980)["severity"] == "critical"
    assert score("checkout", 980)["severity"] == "critical"
    assert score("checkout", 124) is None


@pytest.mark.parametrize("value", [True, "120", float("inf"), float("nan")])
def test_invalid_values_are_rejected(value):
    with pytest.raises(ValueError, match="Invalid metric value"):
        Scorer().score(json.dumps({"metric": "checkout", "value": value}))
