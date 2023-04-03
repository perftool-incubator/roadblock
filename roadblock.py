'''module for roadblock capabilities'''

class roadblock:
    '''roadblock object class'''

    def __init__(self, myvar):
        '''roadblock object initiator function'''

        self.myvar = myvar

    def get_myvar(self):
        '''roadblock object accessor function'''

        return self.myvar

    def set_myvar(self, tmpvar):
        '''roadblock object set function'''

        self.myvar = tmpvar
