__author__ = 'O.Koren'


import time
from Operations import Operation


def run_connection_test():
    test_operation = Operation()
    return test_operation.test_connection()


def setup():
    setup_operation = Operation()
    setup_operation.set_ads_table()


def run_operation1(result_table):
    operation1 = Operation(result_table=result_table)
    operation1.sort_operation(1)


def run_operation2(result_table):
    operation2 = Operation(result_table=result_table)
    operation2.sort_operation(2)


def run_operation3(result_table):
    operation3 = Operation(result_table=result_table)
    operation3.sort_operation(3)


def query_results(result_table):
    results_operation = Operation(result_table=result_table)
    results_operation.select_results()


def close_connection():
    close_operation = Operation()
    close_operation.close_connection()


if __name__ == '__main__':
    result = run_connection_test()
    if result:
        setup()
        run_operation1('RESULTS')
        run_operation2('RESULTS')
        run_operation3('RESULTS')
        query_results('RESULTS')
        time.sleep(10)
        close_connection()
