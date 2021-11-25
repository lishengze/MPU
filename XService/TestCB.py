class A:
    def __init__(self, B):
        print(B._name)
        
class B:
    def __init__(self):
        self._name = "B"
        self._a = A(self)
    
def test():
    b = B()
    
if __name__ == '__main__':
    test()