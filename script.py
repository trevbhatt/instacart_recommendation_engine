from recommendation import test_suggestions


test_suggestions('bananas', 0, 15, n_recs=10, percent=True)


test_suggestions('strawberries', 5, 10, n_recs=10, percent=True)


test_suggestions('chicken', 6, 19, n_recs=10, percent=True)


test_suggestions('milk', 3, 0, n_recs=10, percent=True)