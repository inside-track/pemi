import re

import pytest

def pytest_generate_tests(metafunc):
    if not hasattr(metafunc.function, 'pytestmark'):
        return

    pymarks = {mark.name: mark for mark in metafunc.function.pytestmark}
    if 'scenario' in pymarks:
        args = pymarks['scenario'].args
        scenario = args[0]

        if len(args) > 1:
            metafunc.config.warn(0, 'WARNING - Only a subset of cases are selected: {}'.format(args))
            re_tag = re.compile(args[1])
            for name, case in list(scenario.cases.items()):
                if not re_tag.match(name):
                    del scenario.cases[name]

        metafunc.parametrize(
            ['case'],
            [[case] for case in scenario.cases.values()],
            ids=[case.name for case in scenario.cases.values()]
        )
