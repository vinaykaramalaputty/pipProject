class Explanation(Exception):
    def __str__(self):
        return "\n" + str(self.args[0])

try:
    raise AssertionError("Failed!")
except Exception as e:
    raise Explanation("You can reproduce this error by ...") from e