import unittest
from unittest import mock
from tap_typeform.streams import fetch_sub_questions
from parameterized import parameterized

class TestSubQuestionTest(unittest.TestCase):
    @parameterized.expand([
       ["Question group have sub-questions",
            {
                'case1a': 'value1a', 'case1b': 'value1b', 'case1c': 'value1c',
                'properties': {
                    'description': 'Group question',
                    'case1': 'value1','case2':'value2',
                    'fields': [
                        {
                            'id': 'id1', 'title': 'title1', 'ref': 'ref1'
                        },
                        {
                            'id': 'id2','title': 'title2', 'ref': 'title2'
                        }
                    ]
                },
                'type': 'group'
            },
           [{'question_id': 'id1', 'title': 'title1', 'ref': 'ref1'}, {'question_id': 'id2', 'title': 'title2', 'ref': 'title2'}]
        ],
       ["Question group don't have sub-questions",
           {
                'case2a': 'value2a', 'case2b': 'value2b', 'case2c': 'value2c',
                'properties': {
                    'description': 'Group question', 'case3':'value3', 'case4':'value4'
                },
                'type': 'group'
            },
           []
       ]
    ])
    def test_fetched_sub_question(self, test_name, test_value, expected_value):
        """
        To verify that we are getting expected response or not for question group
        """

        self.assertEqual(expected_value,  fetch_sub_questions(test_value))

