from unittest import TestCase

from attack_corpus import ALLOWED, CORPUS
from script_check import ScriptRejected, check_script


class TestScriptCheck(TestCase):
    def test_corpus(self):
        for name, (code, accepted) in CORPUS.items():
            with self.subTest(name):
                if accepted:
                    self.assertEqual(
                        [("logs", "FilterLogEvents")], check_script(code, ALLOWED)
                    )
                else:
                    with self.assertRaises(ScriptRejected):
                        check_script(code, ALLOWED)

    def test_region_must_be_allowed_literal(self):
        code = (
            'result = await call_boto3(service_name="logs", operation_name="FilterLogEvents", '
            'region_name="eu-west-1", params={})\nresult'
        )
        with self.assertRaises(ScriptRejected):
            check_script(code, ALLOWED, allowed_regions=["us-west-2"])
        check_script(code, ALLOWED, allowed_regions=["eu-west-1"])

    def test_gather_pattern_allowed(self):
        code = (
            "rs = await asyncio.gather(*[call_boto3(service_name='logs', operation_name='FilterLogEvents', "
            "params={'logGroupName': g}) for g in ['a', 'b']], return_exceptions=True)\n"
            "result = {'n': len(rs)}\nresult"
        )
        check_script(code, ALLOWED)
