def mock_os(*args, **kwargs):
    class TestOS:
        @classmethod
        def execv(cls, *args, **kwargs):
            return True

    test_os = TestOS()

    return test_os
