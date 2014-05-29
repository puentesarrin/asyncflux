# -*- coding: utf-8 -*-
import os
import sys
from unittest import defaultTestLoader, TextTestRunner, TestSuite

TESTS = ('asyncflux_test', 'client_test', 'database_test',
         'clusteradmins_test', 'users_test', 'util_test', )


def make_suite(prefix='', extra=(), force_all=False):
    tests = TESTS + extra
    test_names = list(prefix + x for x in tests)
    suite = TestSuite()
    suite.addTest(defaultTestLoader.loadTestsFromNames(test_names))
    return suite


def additional_tests():
    """
    This is called automatically by setup.py test
    """
    return make_suite('tests.')


def main():
    my_dir = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, os.path.abspath(os.path.join(my_dir, '..')))

    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option('--with-pep8', action='store_true', dest='with_pep8',
                      default=True)
    parser.add_option('--force-all', action='store_true', dest='force_all',
                      default=False)
    parser.add_option('-v', '--verbose', action='count', dest='verbosity',
                      default=0)
    parser.add_option('-q', '--quiet', action='count', dest='quietness',
                      default=0)
    options, extra_args = parser.parse_args()
    has_pep8 = False
    try:
        import pep8
        has_pep8 = True
    except ImportError:
        if options.with_pep8:
            sys.stderr.write('# Could not find pep8 library.')
            sys.exit(1)

    if has_pep8:
        guide_main = pep8.StyleGuide(
            ignore=[],
            paths=['asyncflux/'],
            exclude=[],
            max_line_length=80,
        )
        guide_tests = pep8.StyleGuide(
            ignore=['E221'],
            paths=['tests/'],
            max_line_length=80,
        )
        for guide in (guide_main, guide_tests):
            report = guide.check_files()
            if report.total_errors:
                sys.exit(1)

    suite = make_suite('', tuple(extra_args), options.force_all)

    runner = TextTestRunner(verbosity=options.verbosity - options.quietness + 1)
    result = runner.run(suite)
    sys.exit(not result.wasSuccessful())

if __name__ == '__main__':
    main()
