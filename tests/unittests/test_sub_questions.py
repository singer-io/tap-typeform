from tap_typeform.streams import fetch_sub_questions

def test_fetched_sub_question():
    """To verify that we are getting expeted response or not for question qroup"""
    
    test_cases = [{'case1a': 'value1a', 'case1b': 'value1b', 'case1c': 'value1c', 'properties': {'description': 'Group question', 'case1': 'value1','case2':'value2', 'fields': [{'id': 'id1', 'title': 'title1', 'ref': 'ref1'}, {'id': 'id2','title': 'title2', 'ref': 'title2'}]}, 'type': 'group'}, {'case2a': 'value2a', 'case2b': 'value2b', 'case2c': 'value2c', 'properties': {'description': 'Group question', 'case3':'value3', 'case4':'value4'}, 'type': 'group'}]
    expected_case = [{'question_id': 'id1', 'title': 'title1', 'ref': 'ref1'}, {'question_id': 'id2', 'title': 'title2', 'ref': 'title2'}]

    for test_case in test_cases :
        if test_case ['properties'].get('fields'):
            assert expected_case == fetch_sub_questions(test_case)
        else:
            assert [] == fetch_sub_questions(test_case)
