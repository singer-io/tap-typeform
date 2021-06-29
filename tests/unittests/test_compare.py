from tap_typeform import _compare_forms


def test_validate_form_ids():
    test_cases = [
        {'case': {'abc', 'def', 'ghi'}, 'valid': {'abc', 'def'}, 'expected': {'ghi'}},
        {'case': {'xyz', 'tuv', 'qrs'}, 'valid': {'xyz'}, 'expected': {'tuv', 'qrs'}},
        {'case': {'one', 'two', 'three'}, 'valid': set(), 'expected': {'one', 'two', 'three'}},
        {'case': {'a', 'b', 'c'}, 'valid': {'a', 'b', 'c'}, 'expected': set()},
    ]

    for test_case in test_cases:
        assert test_case['expected'] == _compare_forms(test_case['case'], test_case['valid'])