from src.detector import detect

def test_flags_extreme_event():
    outcome = detect(100, [10, 11, 9, 10, 11])
    assert outcome.alert is True
    assert outcome.severity == "critical"
